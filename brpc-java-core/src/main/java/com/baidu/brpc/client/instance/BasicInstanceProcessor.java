/*
 * Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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

package com.baidu.brpc.client.instance;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.baidu.brpc.client.channel.BrpcChannel;
import com.baidu.brpc.client.channel.BrpcChannelFactory;
import com.baidu.brpc.client.RpcClient;

public class BasicInstanceProcessor implements InstanceProcessor<BrpcChannel> {
    private CopyOnWriteArraySet<ServiceInstance> instances;
    private CopyOnWriteArrayList<BrpcChannel> instanceChannels;
    private ConcurrentMap<ServiceInstance, BrpcChannel> instanceChannelMap;
    private Lock lock;
    private RpcClient rpcClient;

    public BasicInstanceProcessor(RpcClient rpcClient) {
        this.instances = new CopyOnWriteArraySet<ServiceInstance>();
        this.instanceChannels = new CopyOnWriteArrayList<BrpcChannel>();
        this.instanceChannelMap = new ConcurrentHashMap<ServiceInstance, BrpcChannel>();
        this.lock = new ReentrantLock();
        this.rpcClient = rpcClient;
    }

    @Override
    public void addInstance(ServiceInstance instance) {
        lock.lock();
        try {
            if (instances.add(instance)) {
                BrpcChannel brpcChannel = BrpcChannelFactory.createChannel(instance, rpcClient);
                instanceChannels.add(brpcChannel);
                instanceChannelMap.putIfAbsent(instance, brpcChannel);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void addInstances(Collection<ServiceInstance> addList) {
        for (ServiceInstance instance : addList) {
            addInstance(instance);
        }
    }

    @Override
    public void deleteInstances(Collection<ServiceInstance> deleteList) {
        for (ServiceInstance instance : deleteList) {
            deleteInstance(instance);
        }
    }

    private void deleteInstance(ServiceInstance instance) {
        lock.lock();
        try {
            if (instances.remove(instance)) {
                Iterator<BrpcChannel> iterator = instanceChannels.iterator();
                while (iterator.hasNext()) {
                    BrpcChannel brpcChannel = iterator.next();
                    if (brpcChannel.getServiceInstance().equals(instance)) {
                        brpcChannel.close();
                        instanceChannels.remove(brpcChannel);
                        instanceChannelMap.remove(instance);
                        break;
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CopyOnWriteArraySet<ServiceInstance> getInstances() {
        return instances;
    }

    @Override
    public CopyOnWriteArrayList<BrpcChannel> getHealthyInstanceChannels() {
        return instanceChannels;
    }

    @Override
    public CopyOnWriteArrayList<BrpcChannel> getUnHealthyInstanceChannels() {
        return instanceChannels;
    }

    @Override
    public ConcurrentMap<ServiceInstance, BrpcChannel> getInstanceChannelMap() {
        return instanceChannelMap;
    }

    @Override
    public void stop() {
        for (BrpcChannel brpcChannel : instanceChannels) {
            brpcChannel.close();
        }
    }
}
