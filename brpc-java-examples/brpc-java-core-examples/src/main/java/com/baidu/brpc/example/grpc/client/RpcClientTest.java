package com.baidu.brpc.example.grpc.client;

import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.instance.Endpoint;
import com.baidu.brpc.client.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.example.standard.Echo;
import com.baidu.brpc.example.standard.EchoServiceGrpc;
import com.baidu.brpc.protocol.Options;
import io.grpc.StatusRuntimeException;

public class RpcClientTest {
    public static void main(String[] args) {
        RpcClientOptions clientOption = new RpcClientOptions();
        clientOption.setProtocolType(Options.ProtocolType.PROTOCOL_GRPC_VALUE);
        clientOption.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
        clientOption.setMaxTryTimes(1);

        Endpoint endpoint = new Endpoint("127.0.0.1",50051);
        RpcClient rpcClient = new RpcClient(endpoint,clientOption);
        io.grpc.Channel channel = rpcClient.selectChannel(endpoint,true);

        EchoServiceGrpc.EchoServiceBlockingStub blockingStub = EchoServiceGrpc.newBlockingStub(channel);
        Echo.EchoRequest request = Echo.EchoRequest.newBuilder().setMessage("hello grpc!!").build();
        Echo.EchoResponse response;
        try {
            for(int i=0;i<10;i++) {
                response = blockingStub.echo(request);
                System.out.println("Greeting: " + response.getMessage());
            }
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
        }
    }
}
