package com.baidu.brpc.client.instance;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class GrpcInstanceProcessor implements InstanceProcessor<ManagedChannel> {
    private CopyOnWriteArraySet<ServiceInstance> instances;
    private CopyOnWriteArrayList<ManagedChannel> instanceChannels;
    private ConcurrentMap<ServiceInstance, ManagedChannel> instanceChannelMap;
    private Lock lock;

    public GrpcInstanceProcessor() {
        this.instances = new CopyOnWriteArraySet<ServiceInstance>();
        this.instanceChannels = new CopyOnWriteArrayList<ManagedChannel>();
        this.instanceChannelMap = new ConcurrentHashMap<ServiceInstance, ManagedChannel>();
        this.lock = new ReentrantLock();
    }

    @Override
    public void addInstance(ServiceInstance instance) {
        lock.lock();
        try {
            if (instances.add(instance)) {
                ManagedChannel grpcChannel = ManagedChannelBuilder.forAddress(instance.getIp(),instance.getPort())
                        .usePlaintext()
                        .build();
                instanceChannels.add(grpcChannel);
                instanceChannelMap.putIfAbsent(instance, grpcChannel);
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
                Iterator<ManagedChannel> iterator = instanceChannels.iterator();
                while (iterator.hasNext()) {
                    ManagedChannel grpcChannel = instanceChannelMap.get(instance);
                    if(grpcChannel.equals(iterator.next())){
                        grpcChannel.shutdown();
                        instanceChannels.remove(grpcChannel);
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
    public CopyOnWriteArrayList<ManagedChannel> getHealthyInstanceChannels() {
        return instanceChannels;
    }

    @Override
    public CopyOnWriteArrayList<ManagedChannel> getUnHealthyInstanceChannels() {
        return instanceChannels;
    }

    @Override
    public ConcurrentMap<ServiceInstance, ManagedChannel> getInstanceChannelMap() {
        return instanceChannelMap;
    }

    @Override
    public void stop() {
        for (ManagedChannel brpcChannel : instanceChannels) {
            brpcChannel.shutdown();
        }
    }
}
