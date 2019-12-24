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

package com.baidu.brpc.client.loadbalance;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.*;

import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.channel.BrpcChannel;
import com.baidu.brpc.client.channel.BrpcChannelFactory;
import com.baidu.brpc.client.instance.ServiceInstance;
import com.baidu.brpc.RpcOptionsUtils;
import com.baidu.brpc.server.RpcServer;

public class RandomStrategyTest {
    private static RpcServer rpcServer1;
    private static RpcServer rpcServer2;
    private static RpcServer rpcServer3;

    private static BrpcChannel instance1;
    private static BrpcChannel instance2;
    private static BrpcChannel instance3;

    private static String serviceUrl = "list://127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002";
    private static RpcClient rpcClient;

    @Before
    public void before() {
        rpcServer1 = new RpcServer(8000, RpcOptionsUtils.getRpcServerOptions());
        rpcServer1.registerService(new LoadBalanceTest.TestEchoService(100));
        rpcServer1.getInterceptors().add(new LoadBalanceTest.TestInterceptor(1));
        rpcServer1.start();
        rpcServer2 = new RpcServer(8001, RpcOptionsUtils.getRpcServerOptions());
        rpcServer2.registerService(new LoadBalanceTest.TestEchoService(200));
        rpcServer2.getInterceptors().add(new LoadBalanceTest.TestInterceptor(2));
        rpcServer2.start();
        rpcServer3 = new RpcServer(8002, RpcOptionsUtils.getRpcServerOptions());
        rpcServer3.registerService(new LoadBalanceTest.TestEchoService(300));
        rpcServer3.getInterceptors().add(new LoadBalanceTest.TestInterceptor(3));
        rpcServer3.start();

        rpcClient = new RpcClient(serviceUrl, RpcOptionsUtils.getRpcClientOptions());
        instance1 = BrpcChannelFactory.createChannel(new ServiceInstance("127.0.0.1", 8000), rpcClient);
        instance2 = BrpcChannelFactory.createChannel(new ServiceInstance("127.0.0.1", 8001), rpcClient);
        instance3 = BrpcChannelFactory.createChannel(new ServiceInstance("127.0.0.1", 8002), rpcClient);
    }

    @After
    public void after() {
        if (instance1 != null) {
            instance1.close();
        }
        if (instance2 != null) {
            instance2.close();
        }
        if (instance3 != null) {
            instance3.close();
        }
        if (rpcClient != null) {
            rpcClient.stop();
        }
        if (rpcServer1 != null) {
            rpcServer1.shutdown();
        }
        if (rpcServer2 != null) {
            rpcServer2.shutdown();
        }
        if (rpcServer3 != null) {
            rpcServer3.shutdown();
        }
    }

    @Test
    public void testSelectInstance() {
        CopyOnWriteArrayList<BrpcChannel> instances = new CopyOnWriteArrayList<BrpcChannel>();
        instances.add(instance1);
        instances.add(instance2);
        instances.add(instance3);
        RandomStrategy randomStrategy = new RandomStrategy();
        BrpcChannel instance = (BrpcChannel)randomStrategy.selectInstance(null, instances, null);
        Assert.assertTrue(instance != null);

        Set<BrpcChannel> selectedInstances = new HashSet<BrpcChannel>();
        selectedInstances.add(instance3);
        instance = (BrpcChannel)randomStrategy.selectInstance(null, instances, selectedInstances);
        Assert.assertTrue(instance.getServiceInstance().getPort() != instance3.getServiceInstance().getPort());

        selectedInstances.add(instance2);
        instance = (BrpcChannel)randomStrategy.selectInstance(null, instances, selectedInstances);
        Assert.assertTrue(instance.getServiceInstance().getPort() == instance1.getServiceInstance().getPort());

        selectedInstances.add(instance1);
        instance = (BrpcChannel)randomStrategy.selectInstance(null, instances, selectedInstances);
        Assert.assertTrue(instance != null);
    }
}
