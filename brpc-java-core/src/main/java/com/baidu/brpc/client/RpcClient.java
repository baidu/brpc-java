/*
 * Copyright (c) 2019 Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.baidu.brpc.client;

import com.baidu.brpc.ChannelInfo;
import com.baidu.brpc.client.channel.BrpcChannel;
import com.baidu.brpc.client.channel.ChannelType;
import com.baidu.brpc.client.handler.IdleChannelHandler;
import com.baidu.brpc.client.handler.RpcClientHandler;
import com.baidu.brpc.client.instance.*;
import com.baidu.brpc.client.loadbalance.LoadBalanceManager;
import com.baidu.brpc.client.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.client.loadbalance.RandomStrategy;
import com.baidu.brpc.exceptions.RpcException;
import com.baidu.brpc.interceptor.ClientTraceInterceptor;
import com.baidu.brpc.interceptor.Interceptor;
import com.baidu.brpc.interceptor.LoadBalanceInterceptor;
import com.baidu.brpc.naming.BrpcURL;
import com.baidu.brpc.naming.ListNamingService;
import com.baidu.brpc.naming.NamingOptions;
import com.baidu.brpc.naming.NamingService;
import com.baidu.brpc.naming.NamingServiceFactory;
import com.baidu.brpc.naming.NamingServiceFactoryManager;
import com.baidu.brpc.naming.NotifyListener;
import com.baidu.brpc.naming.SubscribeInfo;
import com.baidu.brpc.protocol.Options;
import com.baidu.brpc.protocol.Protocol;
import com.baidu.brpc.protocol.ProtocolManager;
import com.baidu.brpc.protocol.Request;
import com.baidu.brpc.server.ServiceManager;
import com.baidu.brpc.spi.ExtensionLoaderManager;
import com.baidu.brpc.thread.BrpcIoThreadPoolInstance;
import com.baidu.brpc.thread.BrpcWorkClientThreadPoolInstance;
import com.baidu.brpc.thread.ClientCallBackThreadPoolInstance;
import com.baidu.brpc.thread.ClientTimeoutTimerInstance;
import com.baidu.brpc.thread.ShutDownManager;
import com.baidu.brpc.utils.BrpcConstants;
import com.baidu.brpc.utils.CustomThreadFactory;
import com.baidu.brpc.utils.ThreadPool;
import com.google.common.collect.Lists;
import io.grpc.ManagedChannel;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import lombok.Getter;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by huwenwei on 2017/4/25.
 */
@SuppressWarnings("unchecked")
@Getter
public class RpcClient {
    private static final Logger LOG = LoggerFactory.getLogger(RpcClient.class);

    private RpcClientOptions rpcClientOptions = new RpcClientOptions();
    private Bootstrap bootstrap;
    private Timer timeoutTimer;
    private Protocol protocol;
    private LoadBalanceStrategy loadBalanceStrategy;
    private List<Interceptor> interceptors = new ArrayList<Interceptor>();
    private LoadBalanceInterceptor loadBalanceInterceptor = new LoadBalanceInterceptor();
    private NamingService namingService;
    private ThreadPool workThreadPool;
    private EventLoopGroup ioThreadPool;
    private Class serviceInterface;
    private SubscribeInfo subscribeInfo;
    private AtomicBoolean stop = new AtomicBoolean(false);
    private InstanceProcessor instanceProcessor;

    /**
     * callBack thread when method invoke fail
     */
    private ExecutorService callbackThread;

    /**
     * 保存单例的引用
     */
    private FastFutureStore fastFutureStore;

    public RpcClient(String namingServiceUrl) {
        this(namingServiceUrl, new RpcClientOptions(), null);
    }

    public RpcClient(String namingServiceUrl, RpcClientOptions options) {
        this(namingServiceUrl, options, null);
    }

    /**
     * parse naming service url, connect to servers
     *
     * @param serviceUrl format like "list://127.0.0.1:8200"
     * @param options    rpc client options
     */
    public RpcClient(String serviceUrl,
                     final RpcClientOptions options,
                     List<Interceptor> interceptors) {
        Validate.notEmpty(serviceUrl);
        Validate.notNull(options);

        ExtensionLoaderManager.getInstance().loadAllExtensions(options.getEncoding());
        // parse naming
        BrpcURL url = new BrpcURL(serviceUrl);
        NamingServiceFactory namingServiceFactory
                = NamingServiceFactoryManager.getInstance().getNamingServiceFactory(url.getSchema());
        this.namingService = namingServiceFactory.createNamingService(url);
        boolean singleServer = false;
        if (namingService instanceof ListNamingService) {
            List<ServiceInstance> instances = namingService.lookup(null);
            singleServer = instances.size() == 1;
        }

        this.init(options, interceptors, singleServer);
    }

    public RpcClient(Endpoint endPoint) {
        this(endPoint, null);
    }

    public RpcClient(Endpoint endPoint, RpcClientOptions options) {
        this(endPoint, options, null);
    }

    public RpcClient(Endpoint endPoint, RpcClientOptions options, List<Interceptor> interceptors) {
        if (null == options) {
            options = new RpcClientOptions();
        }
        ExtensionLoaderManager.getInstance().loadAllExtensions(options.getEncoding());
        this.init(options, interceptors, true);
        instanceProcessor.addInstance(new ServiceInstance(endPoint));
    }

    public RpcClient(List<Endpoint> endPoints) {
        this(endPoints, new RpcClientOptions(), null);
    }

    public RpcClient(List<Endpoint> endPoints, RpcClientOptions options, List<Interceptor> interceptors) {
        ExtensionLoaderManager.getInstance().loadAllExtensions(options.getEncoding());
        this.init(options, interceptors, endPoints.size() == 1);
        for (Endpoint endpoint : endPoints) {
            instanceProcessor.addInstance(new ServiceInstance(endpoint));
        }
    }

    public static <T> T getProxy(RpcClient rpcClient, Class clazz, NamingOptions namingOptions) {
        return BrpcProxy.getProxy(rpcClient, clazz, namingOptions);
    }

    public static <T> T getProxy(RpcClient rpcClient, Class clazz) {
        return BrpcProxy.getProxy(rpcClient, clazz, null);
    }

    /**
     * @param service
     */
    public void registerPushService(Object service) {
        ServiceManager.getInstance().registerPushService(service);

        // 如果只注册了pushService，没有注册一个普通的服务的话， 报错
        if (instanceProcessor.getInstances().size() == 0) {
            LOG.error("there should be have normal services before register push service.");
            throw new RpcException("there should be have normal services before register push service");
        }
    }

    public <T> T getProxy(Class clazz, NamingOptions namingOptions) {
        return BrpcProxy.getProxy(this, clazz, namingOptions);
    }

    public <T> T getProxy(Class clazz) {
        return BrpcProxy.getProxy(this, clazz, null);
    }

    public void setServiceInterface(Class clazz) {
        setServiceInterface(clazz, null);
    }

    public void setServiceInterface(Class clazz, NamingOptions namingOptions) {
        if (this.serviceInterface != null) {
            throw new RpcException("serviceInterface must not be set repeatedly, please use another RpcClient");
        }
        if (clazz.getInterfaces().length == 0) {
            this.serviceInterface = clazz;
        } else {
            // if it is async interface, we should subscribe the sync interface
            this.serviceInterface = clazz.getInterfaces()[0];
        }

        if (namingService != null) {
            if (namingOptions != null) {
                subscribeInfo = new SubscribeInfo(namingOptions);
            } else {
                subscribeInfo = new SubscribeInfo();
            }
            subscribeInfo.setInterfaceName(serviceInterface.getName());
            List<ServiceInstance> instances = this.namingService.lookup(subscribeInfo);
            instanceProcessor.addInstances(instances);
            this.namingService.subscribe(subscribeInfo, new NotifyListener() {
                @Override
                public void notify(Collection<ServiceInstance> addList,
                                   Collection<ServiceInstance> deleteList) {
                    instanceProcessor.addInstances(addList);
                    instanceProcessor.deleteInstances(deleteList);
                }
            });
        }
    }

    public void shutdown() {
        stop();
    }

    public void stop() {
        // avoid stop multi times
        if (stop.compareAndSet(false, true)) {
            if (namingService != null) {
                namingService.unsubscribe(subscribeInfo);
                namingService.destroy();
            }
            if (instanceProcessor != null) {
                instanceProcessor.stop();
            }
            if (loadBalanceStrategy != null) {
                loadBalanceStrategy.destroy();
            }
            if (ioThreadPool != null && !rpcClientOptions.isGlobalThreadPoolSharing()) {
                ioThreadPool.shutdownGracefully().syncUninterruptibly();
            }
            if (workThreadPool != null && !rpcClientOptions.isGlobalThreadPoolSharing()) {
                workThreadPool.stop();
            }
        }
    }

    public boolean isShutdown() {
        return stop.get();
    }

    /**
     * select instance by load balance and select channel from the instance.
     * when user call this function explicitly, he should return the channel to avoid connection leak.
     *
     * @return netty channel
     */
    public Channel selectChannel(Request request) {
        BrpcChannel brpcChannel = (BrpcChannel) loadBalanceStrategy.selectInstance(
                request,
                instanceProcessor.getHealthyInstanceChannels(),
                request.getSelectedInstances());
        if (brpcChannel == null) {
            LOG.debug("no available healthy server, so random select one unhealthy server");
            RandomStrategy randomStrategy = new RandomStrategy();
            randomStrategy.init(this);
            brpcChannel = (BrpcChannel) randomStrategy.selectInstance(
                    request,
                    instanceProcessor.getUnHealthyInstanceChannels(),
                    request.getSelectedInstances());
            if (brpcChannel == null) {
                throw new RpcException(RpcException.NETWORK_EXCEPTION, "no available instance");
            }
        }
        Channel channel;
        try {
            channel = brpcChannel.getChannel();
        } catch (NoSuchElementException full) {
            int maxConnections = brpcChannel.getCurrentMaxConnection() * 2;
            brpcChannel.updateMaxConnection(maxConnections);
            String errMsg = String.format("channel pool is exhausted, and double maxTotalConnection,server=%s:%d",
                    brpcChannel.getServiceInstance().getIp(), brpcChannel.getServiceInstance().getPort());
            LOG.debug(errMsg);
            throw new RpcException(RpcException.NETWORK_EXCEPTION, errMsg, full);
        } catch (IllegalStateException illegalState) {
            String errMsg = String.format("channel pool is closed, server=%s:%d",
                    brpcChannel.getServiceInstance().getIp(), brpcChannel.getServiceInstance().getPort());
            LOG.debug(errMsg);
            throw new RpcException(RpcException.UNKNOWN_EXCEPTION, errMsg, illegalState);
        } catch (Exception connectedFailed) {
            String errMsg;
            if (rpcClientOptions.getChannelType() == ChannelType.POOLED_CONNECTION) {
                errMsg = String.format("channel pool make new object failed, "
                                + "active=%d,idle=%d,server=%s:%d, ex=%s",
                        brpcChannel.getActiveConnectionNum(),
                        brpcChannel.getIdleConnectionNum(),
                        brpcChannel.getServiceInstance().getIp(),
                        brpcChannel.getServiceInstance().getPort(),
                        connectedFailed.getMessage());
            } else {
                errMsg = String.format("get channel failed, ex=%s", connectedFailed.getMessage());
            }
            LOG.debug(errMsg);
            throw new RpcException(RpcException.UNKNOWN_EXCEPTION, errMsg, connectedFailed);
        }

        if (channel == null) {
            String errMsg = "channel is null, retry another channel";
            LOG.debug(errMsg);
            throw new RpcException(RpcException.UNKNOWN_EXCEPTION, errMsg);
        }
        if (!channel.isActive()) {
            brpcChannel.incFailedNum();
            // 如果连接不是有效的，从连接池中剔除。
            brpcChannel.removeChannel(channel);
            String errMsg = "channel is non active, retry another channel";
            throw new RpcException(RpcException.NETWORK_EXCEPTION, errMsg);
        }
        return channel;
    }

    /**
     * select channel from endpoint which is selected by custom load balance.
     *
     * @param endpoint ip:port
     * @return netty channel
     */
    public Channel selectChannel(Endpoint endpoint) {
        BrpcChannel brpcChannel = (BrpcChannel)instanceProcessor.getInstanceChannelMap().get(endpoint);
        if (brpcChannel == null) {
            LOG.warn("instance:{} not found, may be it is removed from naming service.", endpoint);
            throw new RpcException(RpcException.SERVICE_EXCEPTION, "instance not found:" + endpoint);
        }
        Channel channel;
        try {
            channel = brpcChannel.getChannel();
        } catch (Exception ex) {
            throw new RpcException(RpcException.NETWORK_EXCEPTION, "select channel failed from " + endpoint, ex);
        }
        if (!channel.isActive()) {
            brpcChannel.incFailedNum();
            // 如果连接不是有效的，从连接池中剔除。
            brpcChannel.removeChannel(channel);
            String errMsg = "channel is non active, retry another channel";
            throw new RpcException(RpcException.NETWORK_EXCEPTION, errMsg);
        }
        return channel;
    }

    /**
     * select channel from endpoint which is selected by custom load balance.
     *
     * @param endpoint ip:port
     * @return grpc channel
     */
    public io.grpc.Channel selectChannel(Endpoint endpoint,boolean isGrpc) {
        if(isGrpc) {
            Set<io.grpc.Channel> selectedChannel = new HashSet<io.grpc.Channel>();
            ManagedChannel grpcChannel = (ManagedChannel) loadBalanceStrategy.selectInstance(
                    null,
                    instanceProcessor.getHealthyInstanceChannels(),
                    selectedChannel
            );
            //ManagedChannel grpcChannel = (ManagedChannel)instanceProcessor.getInstanceChannelMap().get(endpoint);
            if (grpcChannel == null) {
                LOG.warn("instance:{} not found, may be it is removed from naming service.", endpoint);
                throw new RpcException(RpcException.SERVICE_EXCEPTION, "instance not found:" + endpoint);
            }
            if (grpcChannel.isShutdown() || grpcChannel.isTerminated()) {
                instanceProcessor.deleteInstances(Lists.newArrayList(new ServiceInstance(endpoint)));
                String errMsg = "channel is non active, retry another channel";
                throw new RpcException(RpcException.NETWORK_EXCEPTION, errMsg);
            }
            return grpcChannel;
        }
        return null;
    }


    public void returnChannel(Channel channel) {
        ChannelInfo channelInfo = ChannelInfo.getClientChannelInfo(channel);
        channelInfo.getChannelGroup().returnChannel(channel);
    }

    public void removeChannel(Channel channel) {
        ChannelInfo channelInfo = ChannelInfo.getClientChannelInfo(channel);
        channelInfo.getChannelGroup().removeChannel(channel);
    }

    public <T> AsyncAwareFuture<T> sendRequest(Request request) {
        // select instance by load balance, and select channel from instance.
        Channel channel = selectChannel(request);
        request.setChannel(channel);
        ChannelInfo channelInfo = ChannelInfo.getClientChannelInfo(request.getChannel());
        BrpcChannel brpcChannel = channelInfo.getChannelGroup();
        protocol.beforeRequestSent(request, this, brpcChannel);

        // encode request
        RpcFuture rpcFuture = RpcFuture.createRpcFuture(request, this);
        channelInfo.setCorrelationId(rpcFuture.getCorrelationId());
        rpcFuture.setChannelInfo(channelInfo);
        request.setRpcFuture(rpcFuture);
        request.setCorrelationId(rpcFuture.getCorrelationId());
        try {
            request.setSendBuf(protocol.encodeRequest(request));
        } catch (Throwable t) {
            throw new RpcException(RpcException.SERIALIZATION_EXCEPTION, t.getMessage(), t);
        }

        // register timeout timer
        RpcTimeoutTimer timeoutTask = new RpcTimeoutTimer(channelInfo, request.getCorrelationId(), this);
        Timeout timeout = timeoutTimer.newTimeout(timeoutTask, request.getReadTimeoutMillis(), TimeUnit.MILLISECONDS);
        request.getRpcFuture().setTimeout(timeout);
        try {
            // netty will release the send buffer after sent.
            // we retain here, so it can be used when rpc retry.
            request.retain();
            ChannelFuture sendFuture = request.getChannel().writeAndFlush(request.getSendBuf());
            sendFuture.awaitUninterruptibly(request.getWriteTimeoutMillis());
            if (!sendFuture.isSuccess()) {
                if (!(sendFuture.cause() instanceof ClosedChannelException)) {
                    LOG.warn("send request failed, channelActive={}, ex=",
                            request.getChannel().isActive(), sendFuture.cause());
                }
                String errMsg = String.format("send request failed, channelActive=%b",
                        request.getChannel().isActive());
                throw new RpcException(RpcException.NETWORK_EXCEPTION, errMsg);
            }
        } catch (Exception ex) {
            channelInfo.handleRequestFail(rpcClientOptions.getChannelType(), request.getCorrelationId());
            timeout.cancel();
            if (ex instanceof RpcException) {
                throw (RpcException) ex;
            } else {
                throw new RpcException(RpcException.SERIALIZATION_EXCEPTION, ex.getMessage(), ex);
            }
        }

        // return channel
        channelInfo.handleRequestSuccess(rpcClientOptions.getChannelType());
        return request.getRpcFuture();
    }

    public void triggerCallback(Runnable runnable) {
        if (!callbackThread.isTerminated()) {
            callbackThread.execute(runnable);
        }
    }

    private void init(final RpcClientOptions options, List<Interceptor> interceptors, boolean singleServer) {
        Validate.notNull(options);
        try {
            this.rpcClientOptions.copyFrom(options);
        } catch (Exception ex) {
            LOG.warn("init rpc options failed, so use default");
        }
        if (interceptors != null) {
            this.interceptors.addAll(interceptors);
        }
        this.interceptors.add(new ClientTraceInterceptor());
        this.protocol = ProtocolManager.getInstance().getProtocol(options.getProtocolType());
        if (!protocol.returnChannelBeforeResponse()
                && rpcClientOptions.getChannelType() == ChannelType.SINGLE_CONNECTION) {
            String errorString = "it can't use SINGLE_CONNECTION when protocol returns channel before response";
            LOG.error(errorString);
            throw new IllegalArgumentException(errorString);
        }
        fastFutureStore = FastFutureStore.getInstance(options.getFutureBufferSize());
        timeoutTimer = ClientTimeoutTimerInstance.getOrCreateInstance();

        // singleServer do not need healthChecker
        if(options.getProtocolType() == Options.ProtocolType.PROTOCOL_GRPC_VALUE) {
            instanceProcessor = new GrpcInstanceProcessor();
        } else if (singleServer) {
            instanceProcessor = new BasicInstanceProcessor(this);
        } else {
            instanceProcessor = new EnhancedInstanceProcessor(this);
        }

        // 负载均衡算法
        loadBalanceStrategy = LoadBalanceManager.getInstance().createLoadBalance(
                rpcClientOptions.getLoadBalanceType());
        loadBalanceStrategy.init(this);

        if(!(options.getProtocolType() == Options.ProtocolType.PROTOCOL_GRPC_VALUE)) {
            //if not grpc
            // init once
        ShutDownManager.getInstance();
        boolean threadPoolSharing = rpcClientOptions.isGlobalThreadPoolSharing();
        if (threadPoolSharing) {
            this.workThreadPool =
                    BrpcWorkClientThreadPoolInstance.getOrCreateInstance(rpcClientOptions.getWorkThreadNum());
            if (rpcClientOptions.getIoEventType() == BrpcConstants.IO_EVENT_NETTY_EPOLL) {
                ioThreadPool = BrpcIoThreadPoolInstance.getOrCreateEpollInstance(options.getIoThreadNum());
            } else {
                ioThreadPool = BrpcIoThreadPoolInstance.getOrCreateNioInstance(options.getIoThreadNum());
            }
        } else {
            this.workThreadPool = new ThreadPool(rpcClientOptions.getWorkThreadNum(),
                    new CustomThreadFactory("client-work-thread"));
            if (rpcClientOptions.getIoEventType() == BrpcConstants.IO_EVENT_NETTY_EPOLL) {
                ioThreadPool = new EpollEventLoopGroup(options.getIoThreadNum(),
                        new CustomThreadFactory("client-io-thread"));
            } else {
                ioThreadPool = new NioEventLoopGroup(options.getIoThreadNum(),
                        new CustomThreadFactory("client-io-thread"));
            }
        }

        this.callbackThread = ClientCallBackThreadPoolInstance.getOrCreateInstance(1);

        // init netty bootstrap
        bootstrap = new Bootstrap();
        if (rpcClientOptions.getIoEventType() == BrpcConstants.IO_EVENT_NETTY_EPOLL) {
            bootstrap.channel(EpollSocketChannel.class);
            bootstrap.option(EpollChannelOption.EPOLL_MODE, EpollMode.EDGE_TRIGGERED);
        } else {
            bootstrap.channel(NioSocketChannel.class);
        }

        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, rpcClientOptions.getConnectTimeoutMillis());
        bootstrap.option(ChannelOption.SO_KEEPALIVE, rpcClientOptions.isKeepAlive());
        bootstrap.option(ChannelOption.SO_REUSEADDR, rpcClientOptions.isReuseAddr());
        bootstrap.option(ChannelOption.TCP_NODELAY, rpcClientOptions.isTcpNoDelay());
        bootstrap.option(ChannelOption.SO_RCVBUF, rpcClientOptions.getReceiveBufferSize());
        bootstrap.option(ChannelOption.SO_SNDBUF, rpcClientOptions.getSendBufferSize());
        final RpcClientHandler rpcClientHandler = new RpcClientHandler(RpcClient.this);
        final ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                if (rpcClientOptions.getChannelType() == ChannelType.SINGLE_CONNECTION) {
                    ch.pipeline().addLast(new IdleStateHandler(0, 0, rpcClientOptions.getKeepAliveTime()));
                    ch.pipeline().addLast(new IdleChannelHandler());
                }
                ch.pipeline().addLast(rpcClientHandler);
            }
        };

        bootstrap.group(ioThreadPool).handler(initializer);
    }}

    public void removeLogId(long id) {
        fastFutureStore.getAndRemove(id);
    }

    public RpcClientOptions getRpcClientOptions() {
        return rpcClientOptions;
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public CopyOnWriteArrayList<BrpcChannel> getHealthyInstances() {
        return instanceProcessor.getHealthyInstanceChannels();
    }

    public List<Interceptor> getInterceptors() {
        return interceptors;
    }

    public Bootstrap getBootstrap() {
        return bootstrap;
    }

    public ThreadPool getWorkThreadPool() {
        return workThreadPool;
    }

    public LoadBalanceStrategy getLoadBalanceStrategy() {
        return loadBalanceStrategy;
    }

    public boolean isLongConnection() {
        return rpcClientOptions.getChannelType() != ChannelType.SHORT_CONNECTION;
    }

    public NamingService getNamingService() {
        return namingService;
    }

    public Timer getTimeoutTimer() {
        return timeoutTimer;
    }

    public LoadBalanceInterceptor getLoadBalanceInterceptor() {
        return loadBalanceInterceptor;
    }

    public void setLoadBalanceInterceptor(LoadBalanceInterceptor loadBalanceInterceptor) {
        this.loadBalanceInterceptor = loadBalanceInterceptor;
    }

    public SubscribeInfo getSubscribeInfo() {
        return subscribeInfo;
    }

    public InstanceProcessor getInstanceProcessor() {
        return instanceProcessor;
    }

    public void setInstanceProcessor(InstanceProcessor instanceProcessor) {
        this.instanceProcessor = instanceProcessor;
    }

}
