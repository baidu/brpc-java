package com.baidu.brpc.example.grpc;

import com.baidu.brpc.protocol.Options;
import com.baidu.brpc.server.RpcServer;
import com.baidu.brpc.server.RpcServerOptions;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by kewei wang on 2019/6/5.
 */
@Slf4j
public class RpcServerTest {
    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
        int port = 50051;
        if (args.length == 1) {
            port = Integer.valueOf(args[0]);
        }

        RpcServerOptions options = new RpcServerOptions();
        options.setReceiveBufferSize(64 * 1024 * 1024);
        options.setSendBufferSize(64 * 1024 * 1024);
        options.setKeepAliveTime(20);
        options.setProtocolType(Options.ProtocolType.PROTOCOL_GRPC_PROTOBUF_VALUE);
//        options.setNamingServiceUrl("zookeeper://127.0.0.1:2181");
        final RpcServer rpcServer = new RpcServer(port, options);
        rpcServer.registerService(new EchoServiceImpl());
        rpcServer.start();

        // make server keep running
        synchronized (RpcServerTest.class) {
            try {
                RpcServerTest.class.wait();
            } catch (Throwable e) {
            }
        }
    }
}