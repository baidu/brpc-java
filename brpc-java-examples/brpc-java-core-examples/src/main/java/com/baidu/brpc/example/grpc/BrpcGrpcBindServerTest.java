package com.baidu.brpc.example.grpc;

import com.baidu.brpc.example.standard.Echo;
import com.baidu.brpc.example.standard.EchoServiceImpl;
import com.baidu.brpc.protocol.Options;
import com.baidu.brpc.server.RpcServerOptions;
import com.baidu.brpc.server.grpc.BrpcGrpcServiceBinder;
import com.baidu.brpc.server.grpc.GrpcServer;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by kewei wang on 2019/6/9.
 * bind brpc service to grpc server
 */
@Slf4j
public class BrpcGrpcBindServerTest {
    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
        int port = 50051;
        if (args.length == 1) {
            port = Integer.valueOf(args[0]);
        }

        RpcServerOptions options = new RpcServerOptions();
        options.setReceiveBufferSize(64 * 1024 * 1024);
        options.setSendBufferSize(64 * 1024 * 1024);
        options.setKeepAliveTime(20);
        options.setProtocolType(Options.ProtocolType.PROTOCOL_GRPC_VALUE);
//        options.setNamingServiceUrl("zookeeper://127.0.0.1:2181");
//        final RpcServer rpcServer = new RpcServer(port, options);
        final GrpcServer rpcServer = new GrpcServer(port, options);
        //rpcServer.registerService(new EchoService());
        BrpcGrpcServiceBinder converter = new BrpcGrpcServiceBinder<Echo.EchoRequest, Echo.EchoResponse>(
                EchoServiceImpl.class,
                Echo.EchoRequest.class,// we need request type because we need to find method in service
                Echo.getDescriptor(),
                "example_for_cpp.EchoService", //grpc meta info
                "Echo",//grpc meta info
                Echo.EchoRequest.getDefaultInstance(),
                Echo.EchoResponse.getDefaultInstance()
        );
        rpcServer.registerService(converter.bindService("echo"));
        rpcServer.start();

        // make server keep running
        synchronized (BrpcGrpcBindServerTest.class) {
            try {
                BrpcGrpcBindServerTest.class.wait();
            } catch (Throwable e) {
            }
        }
    }
}