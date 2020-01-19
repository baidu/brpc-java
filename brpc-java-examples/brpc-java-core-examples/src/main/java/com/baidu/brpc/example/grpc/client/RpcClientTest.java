package com.baidu.brpc.example.grpc.client;

import com.baidu.brpc.RpcContext;
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.example.interceptor.CustomInterceptor;
import com.baidu.brpc.example.standard.Echo;
import com.baidu.brpc.example.grpc.EchoService;
import com.baidu.brpc.exceptions.RpcException;
import com.baidu.brpc.interceptor.Interceptor;
import com.baidu.brpc.protocol.Options;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
@Slf4j
public class RpcClientTest {
    public static void main(String[] args) {
        RpcClientOptions clientOption = new RpcClientOptions();
        clientOption.setProtocolType(Options.ProtocolType.PROTOCOL_GRPC_PROTOBUF_VALUE);
        clientOption.setWriteTimeoutMillis(1000);
        clientOption.setReadTimeoutMillis(5000);
        clientOption.setMaxTotalConnections(1);
        clientOption.setMinIdleConnections(10);
        clientOption.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
        clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);

        String serviceUrl = "list://127.0.0.1:50051";
//        String serviceUrl = "zookeeper://127.0.0.1:2181";
        if (args.length == 1) {
            serviceUrl = args[0];
        }



        List<Interceptor> interceptors = new ArrayList<Interceptor>();
        interceptors.add(new CustomInterceptor());

        // sync call
        RpcClient rpcClient = new RpcClient(serviceUrl, clientOption, interceptors);
//        RpcClient rpcClient = new RpcClient(serviceUrl, clientOption, interceptors);
        EchoService echoService = BrpcProxy.getProxy(rpcClient, EchoService.class);
        RpcContext.getContext().setLogId(1234);

        try {
            for(int i = 0;i<100;i++) {

                // build request
                Echo.EchoRequest request = Echo.EchoRequest.newBuilder()
                        .setMessage("helloooooooooooo"+ (i+1))
                        .build();

                Echo.EchoResponse response = echoService.echo(request);
                System.out.println("Call times: "+ (i+1));
                System.out.printf("sync call service=EchoService.echo success, "
                                + "request=%s,response=%s\n",
                        request.getMessage(), response.getMessage());
            }
        } catch (RpcException ex) {
            log.warn("sync call failed, ex=", ex);
        }
        rpcClient.stop();

    }
}
