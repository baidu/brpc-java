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

import com.baidu.brpc.client.channel.BrpcChannel;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

public interface InstanceProcessor<T> {
    void addInstance(ServiceInstance instance);

    void addInstances(Collection<ServiceInstance> addList);

    void deleteInstances(Collection<ServiceInstance> deleteList);

    CopyOnWriteArraySet<ServiceInstance> getInstances();

    CopyOnWriteArrayList<T> getHealthyInstanceChannels();

    CopyOnWriteArrayList<T> getUnHealthyInstanceChannels();

    ConcurrentMap<ServiceInstance,T> getInstanceChannelMap();

    void stop();

}
