package com.baidu.brpc.example.grpc.client;


import com.baidu.brpc.example.standard.Echo;
import com.baidu.brpc.example.standard.EchoServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.TimeUnit;

public class EchoGrpcClient {
    private static final Logger logger = Logger.getLogger(EchoGrpcClient.class.getName());

    private final ManagedChannel channel;
    private final EchoServiceGrpc.EchoServiceBlockingStub blockingStub;

    /**
     * Construct client connecting to HelloWorld server at {@code host:port}.
     */
    public EchoGrpcClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build());
    }

    /**
     * Construct client for accessing HelloWorld server using the existing channel.
     */
    EchoGrpcClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = EchoServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * Say hello to server.
     */
    public void greet(String name) {
        System.out.println("Will try to greet " + name + " ...");
        Echo.EchoRequest request = Echo.EchoRequest.newBuilder().setMessage(name).build();
        Echo.EchoResponse response;
        try {
            response = blockingStub.echo(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING,"RPC failed: {0}", e.getStatus());
            return;
        }
        System.out.println("Greeting: " + response.getMessage());
    }

    /**
     * Greet server. If provided, the first element of {@code args} is the name to use in the
     * greeting.
     */
    public static void main(String[] args) throws Exception {
        EchoGrpcClient client = new EchoGrpcClient("localhost", 50051);
        try {
            /* Access a service running on the local machine on port 50051 */
            String user = "world";
            if (args.length > 0) {
                user = args[0]; /* Use the arg as the name to greet if provided */
            }
            for(int i = 0; i < 5; i++)
            client.greet(user);
        } finally {
            client.shutdown();
        }
    }
}
  