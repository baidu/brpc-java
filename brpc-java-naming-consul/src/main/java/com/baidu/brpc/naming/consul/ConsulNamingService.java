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

package com.baidu.brpc.naming.consul;

import com.baidu.brpc.client.instance.ServiceInstance;
import com.baidu.brpc.exceptions.RpcException;
import com.baidu.brpc.naming.*;
import com.baidu.brpc.utils.CustomThreadFactory;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.health.HealthServicesRequest;
import com.ecwid.consul.v1.health.model.HealthService;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.internal.ConcurrentSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.concurrent.*;

@Slf4j
public class ConsulNamingService implements NamingService {
    private BrpcURL url;
    private ConsulClient client;
    private int retryInterval;
    private int consulInterval;
    private int lookupInterval;
    private ConcurrentSet<RegisterInfo> failedRegisters = new ConcurrentSet<RegisterInfo>();
    private ConcurrentSet<RegisterInfo> failedUnregisters = new ConcurrentSet<RegisterInfo>();

    private ConcurrentMap<SubscribeInfo, NotifyListener> failedSubscribes
            = new ConcurrentHashMap<SubscribeInfo, NotifyListener>();
    private ConcurrentSet<SubscribeInfo> failedUnsubscribes
            = new ConcurrentSet<SubscribeInfo>();
    private ConcurrentMap<SubscribeInfo, WatchTask> watchTaskMap
            = new ConcurrentHashMap<SubscribeInfo, WatchTask>();
    private Timer retryTimer;

    private Set<String> instanceIds = new ConcurrentSet<String>();
    private ScheduledExecutorService heartbeatExecutor;
    private ExecutorService watchExecutor;

    public ConsulNamingService(BrpcURL url) {
        this.url = url;
        try {
            String[] hostPorts = url.getHostPorts().split(":");
            this.client = new ConsulClient(hostPorts[0], Integer.parseInt(hostPorts[1]));
        } catch (Exception e) {
            throw new RpcException(RpcException.SERVICE_EXCEPTION,
                    "wrong configuration of url, should be like test.bj:port", e);
        }

        this.retryInterval = url.getIntParameter(Constants.INTERVAL, Constants.DEFAULT_INTERVAL);
        this.consulInterval = url.getIntParameter(ConsulConstants.CONSULINTERVAL,
                ConsulConstants.DEFAULT_CONSUL_INTERVAL);
        this.lookupInterval = url.getIntParameter(ConsulConstants.LOOKUPINTERVAL,
                ConsulConstants.DEFAULT_LOOKUP_INTERVAL);
        retryTimer = new HashedWheelTimer(new CustomThreadFactory("consul-retry-timer-thread"));
        retryTimer.newTimeout(
                new TimerTask() {
                    @Override
                    public void run(Timeout timeout) throws Exception {
                        try {
                            for (RegisterInfo registerInfo : failedRegisters) {
                                register(registerInfo);
                            }
                            for (RegisterInfo registerInfo : failedUnregisters) {
                                unregister(registerInfo);
                            }
                            for (Map.Entry<SubscribeInfo, NotifyListener> entry : failedSubscribes.entrySet()) {
                                subscribe(entry.getKey(), entry.getValue());
                            }
                            for (SubscribeInfo subscribeInfo : failedUnsubscribes) {
                                unsubscribe(subscribeInfo);
                            }
                        } catch (Exception ex) {
                            log.warn("retry timer exception:", ex);
                        }
                        retryTimer.newTimeout(this, retryInterval, TimeUnit.MILLISECONDS);
                    }
                },
                retryInterval, TimeUnit.MILLISECONDS);

        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(
                new CustomThreadFactory("consul-heartbeat"));
        heartbeatExecutor.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        for (String instanceId : instanceIds) {
                            client.agentCheckPass(instanceId);
                            log.debug("Sending consul heartbeat for: {}", instanceId);
                        }
                    }
                },
                ConsulConstants.HEARTBEAT_CIRCLE,
                ConsulConstants.HEARTBEAT_CIRCLE, TimeUnit.MILLISECONDS);
        watchExecutor = Executors.newFixedThreadPool(1, new CustomThreadFactory("consul-watch"));
    }

    @Override
    public void destroy() {
        retryTimer.stop();
        heartbeatExecutor.shutdown();
        watchExecutor.shutdownNow();
        instanceIds.clear();
    }

    @Override
    public List<ServiceInstance> lookup(SubscribeInfo subscribeInfo) {
        try {
            Response<List<HealthService>> consulServices = lookupHealthService(
                    generateServiceName(subscribeInfo), -1);
            List<ServiceInstance> instances = convert(consulServices);
            log.info("lookup {} instances from consul", instances.size());
            return instances;
        } catch (Exception ex) {
            log.warn("lookup endpoint list failed from {}, msg={}",
                    url, ex.getMessage());
            if (!subscribeInfo.isIgnoreFailOfNamingService()) {
                throw new RpcException("lookup endpoint list failed from consul failed", ex);
            } else {
                return new ArrayList<ServiceInstance>();
            }
        }
    }

    @Override
    public void subscribe(final SubscribeInfo subscribeInfo, final NotifyListener listener) {
        try {
            final String serviceName = generateServiceName(subscribeInfo);
            Response<List<HealthService>> response = lookupHealthService(serviceName, -1);
            List<ServiceInstance> instances = convert(response);
            log.info("lookup {} instances from consul", instances.size());
            WatchTask watchTask = new WatchTask(serviceName, instances, response.getConsulIndex(), listener);
            watchExecutor.submit(watchTask);
            watchTaskMap.putIfAbsent(subscribeInfo, watchTask);
            failedSubscribes.remove(subscribeInfo);
        } catch (Exception ex) {
            log.warn("lookup endpoint list failed from {}, msg={}",
                    url, ex.getMessage());
            if (!subscribeInfo.isIgnoreFailOfNamingService()) {
                throw new RpcException("lookup endpoint list failed from consul failed", ex);
            } else {
                failedSubscribes.putIfAbsent(subscribeInfo, listener);
            }
        }
    }

    @Override
    public void unsubscribe(SubscribeInfo subscribeInfo) {
        try {
            WatchTask watchTask = watchTaskMap.remove(subscribeInfo);
            if (watchTask != null) {
                watchTask.stop();
            }
            log.info("unsubscribe success from {}", url);
        } catch (Exception ex) {
            if (!subscribeInfo.isIgnoreFailOfNamingService()) {
                throw new RpcException("unsubscribe failed from " + url, ex);
            } else {
                failedUnsubscribes.add(subscribeInfo);
                return;
            }
        }
    }

    @Override
    public void register(RegisterInfo registerInfo) {
        try {
            NewService newService = getConsulNewService(registerInfo);
            client.agentServiceRegister(newService);
            instanceIds.add("service:" + newService.getId());
            log.info("register success to {}", url);
        } catch (Exception ex) {
            if (!registerInfo.isIgnoreFailOfNamingService()) {
                throw new RpcException("Failed to register to " + url, ex);
            } else {
                failedRegisters.add(registerInfo);
                return;
            }
        }
        failedRegisters.remove(registerInfo);
    }

    @Override
    public void unregister(RegisterInfo registerInfo) {
        try {
            NewService newService = getConsulNewService(registerInfo);
            client.agentServiceDeregister(newService.getId());
            instanceIds.remove("service:" + newService.getId());
        } catch (Exception ex) {
            if (!registerInfo.isIgnoreFailOfNamingService()) {
                throw new RpcException("Failed to unregister to " + url, ex);
            } else {
                failedUnregisters.add(registerInfo);
                return;
            }
        }
        failedUnregisters.remove(registerInfo);
    }

    private NewService getConsulNewService(RegisterInfo registerInfo) {
        NewService newService = new NewService();
        newService.setName(generateServiceName(registerInfo));
        newService.setId(generateInstanceId(registerInfo));
        newService.setAddress(registerInfo.getHost());
        newService.setPort(registerInfo.getPort());
        newService.setTags(Arrays.asList(ConsulConstants.CONSUL_SERVICE_TAG));

        NewService.Check check = new NewService.Check();
        check.setTtl(this.consulInterval + "s");
        check.setDeregisterCriticalServiceAfter("3m");
        newService.setCheck(check);

        return newService;
    }

    public String generateServiceName(RegisterInfo registerInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append(registerInfo.getGroup())
                .append(":")
                .append(registerInfo.getInterfaceName())
                .append(":")
                .append(registerInfo.getVersion());
        return sb.toString();
    }

    public String generateServiceName(SubscribeInfo subscribeInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append(subscribeInfo.getGroup())
                .append(":")
                .append(subscribeInfo.getInterfaceName())
                .append(":")
                .append(subscribeInfo.getVersion());
        return sb.toString();
    }

    public String generateInstanceId(RegisterInfo registerInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append(generateServiceName(registerInfo))
                .append(":")
                .append(registerInfo.getHost())
                .append(":")
                .append(registerInfo.getPort());
        return sb.toString();
    }

    public Response<List<HealthService>> lookupHealthService(String serviceName, long lastConsulIndex) {
        QueryParams queryParams = new QueryParams(
                ConsulConstants.CONSUL_BLOCK_TIME_SECONDS, lastConsulIndex);
        HealthServicesRequest request = HealthServicesRequest.newBuilder()
                .setTag(ConsulConstants.CONSUL_SERVICE_TAG)
                .setQueryParams(queryParams)
                .setPassing(true)
                .build();
        Response<List<HealthService>> response = client.getHealthServices(serviceName, request);
        return response;
    }

    public List<ServiceInstance> convert(Response<List<HealthService>> consulServices) {
        if (consulServices == null || consulServices.getValue() == null || consulServices.getValue().isEmpty()) {
            return new ArrayList<ServiceInstance>();
        } else {
            List<ServiceInstance> serviceInstances = new ArrayList<ServiceInstance>();
            for (HealthService consulService : consulServices.getValue()) {
                ServiceInstance serviceInstance = new ServiceInstance();
                serviceInstance.setIp(consulService.getService().getAddress());
                serviceInstance.setPort(consulService.getService().getPort());
                serviceInstances.add(serviceInstance);
            }
            return serviceInstances;
        }
    }

    private class WatchTask implements Runnable {
        private String serviceName;
        private List<ServiceInstance> lastInstances = new ArrayList<ServiceInstance>();
        private Long lastConsulIndex = -1L;
        private volatile boolean stopWatch = false;
        private NotifyListener listener;

        public WatchTask(String serviceName,
                         List<ServiceInstance> lastInstances,
                         Long lastConsulIndex,
                         NotifyListener listener) {
            this.serviceName = serviceName;
            this.lastInstances = lastInstances;
            this.lastConsulIndex = lastConsulIndex;
            this.listener = listener;
        }

        @Override
        public void run() {
            while (!stopWatch) {
                Response<List<HealthService>> response = lookupHealthService(serviceName, lastConsulIndex);
                Long currentIndex = response.getConsulIndex();
                if (currentIndex != null && currentIndex > lastConsulIndex) {
                    List<ServiceInstance> currentInstances = convert(response);
                    Collection<ServiceInstance> addList = CollectionUtils.subtract(
                            currentInstances, lastInstances);
                    Collection<ServiceInstance> deleteList = CollectionUtils.subtract(
                            lastInstances, currentInstances);
                    listener.notify(addList, deleteList);
                    lastInstances = currentInstances;
                    lastConsulIndex = currentIndex;
                }
            }
        }

        public void stop() {
            stopWatch = true;
        }
    }

}
