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

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.channel.BrpcChannel;
import com.baidu.brpc.protocol.Request;
import com.baidu.brpc.utils.CollectionUtils;
import com.baidu.brpc.utils.CustomThreadFactory;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import lombok.extern.slf4j.Slf4j;

/**
 * Fair load balance strategy aims to more reasonable distribution of traffic.
 * The principle of traffic distribution is not the same as pv, but the distribution of traffic according to performance,
 * the performance of the machine with multi-point traffic, the poor performance of the machine with less point traffic,
 * the same machine traffic with the same performance, and finally the service performance of all nodes tends to balance,
 * there will be no slow nodes to improve the overall performance of the service.
 *
 * <p>Algorithm overview:
 * The fair load balancing policy dynamically adjusts the weight of each server and divides it by weight.
 * Rpc client maintains a latency window record rpc time for each server, after processing a traffic, the server
 * updates its own window. If the mean value of the service in the window > the overall mean of the service, reduce its own weight.
 * On the contrary, increase its own weight. When the window is not full, the polling does not adjust the weight.
 *
 * @author wangjiayin
 * @since 2018-09-03
 */
@Slf4j
public class FairStrategy<T> implements LoadBalanceStrategy<T> {

    private static final int TIMER_DELAY = 60;

    /**
     * The binary tree used to save weight number for each node.
     * We use a CopyOnWriteArrayList to safely lockless update the weight tree,
     * The first element fo the CopyOnWriteArrayList is useful.
     * The leaf elements of the weight tree are the real rpc servers, and the parent elements of the weight used
     * to save the sum weight of its left and right child's weight.
     * The root element's weight sum is the total sum of every leaf element's weight.
     *
     * <p>The weight tree can achieve time complexity at the O(logN) level, and 1000 servers require only 11 memory accesses.
     */
    private CopyOnWriteArrayList<Node> treeContainer;

    private volatile Timer timer;

    private RpcClient rpcClient;

    private int latencyWindowSize;

    // {@see RpcClientOptions#activeInstancesRatioOfFairLoadBalance}
    private float activeInstancesRatio;

    // fair strategy will not work if the instances is less the minInstancesNum
    private int minInstancesNum = 3;

    private CopyOnWriteArrayList<T> invalidInstances;

    private Random random = new Random(System.currentTimeMillis());

    @Override
    public void init(RpcClient rpcClient) {
        if (timer == null) {
            synchronized (this) {
                if (timer == null) {
                    timer = new HashedWheelTimer(new CustomThreadFactory("fairStrategy-timer-thread"));
                    timer.newTimeout(new TimerTask() {
                        @Override
                        public void run(Timeout timeout) {
                            updateWeightTree();
                            timer.newTimeout(this, TIMER_DELAY, TimeUnit.SECONDS);
                        }
                    }, TIMER_DELAY, TimeUnit.SECONDS);
                    this.rpcClient = rpcClient;
                    treeContainer = new CopyOnWriteArrayList<Node>();
                    invalidInstances = new CopyOnWriteArrayList<T>();
                    latencyWindowSize = rpcClient.getRpcClientOptions().getLatencyWindowSizeOfFairLoadBalance();
                    activeInstancesRatio = rpcClient.getRpcClientOptions().getActiveInstancesRatioOfFairLoadBalance();
                    if (latencyWindowSize <= 1) {
                        throw new IllegalArgumentException("latencyWindowSize must be greater than 1");
                    }
                }
            }
        }
    }

    @Override
    public T selectInstance(
            Request request,
            List<T> instances,
            Set<T> selectedInstances) {

        if (treeContainer.size() == 0) {
            return randomSelect(instances);
        }

        try {
            Node root = treeContainer.get(0);
            T selectedChannelGroup = fairSelect(root);
            // the invalidInstances list size is not very large.
            if (invalidInstances.contains(selectedChannelGroup)) {
                // if the selected node is an invalid one, means the weight tree has not yet updated.
                // random reselect a new one
                log.debug("the selected one is invalid, begin to random reselect a new one...");
                return randomSelect(instances);
            }
            return selectedChannelGroup;
        } catch (Exception e) {
            log.warn("FairStrategy select channel failed.", e);
            return randomSelect(instances);
        }

    }

    @Override
    public void destroy() {
        if (timer != null) {
            timer.stop();
        }
    }

    /**
     * Since the weight tree will update by a period of time, so if there's any invalid instance,
     * the business should notify the fair strategy.
     */
    public void markInvalidInstance(List<T> instances) {
        this.invalidInstances.addAll(instances);
    }

    protected T randomSelect(List<T> instances) {
        long instanceNum = instances.size();
        if (instanceNum == 0) {
            return null;
        }
        int index = (int) (getRandomLong() % instanceNum);
        return instances.get(index);
    }

    protected long getRandomLong() {
        long randomIndex = random.nextLong();
        if (randomIndex < 0) {
            randomIndex = 0 - randomIndex;
        }
        return randomIndex;
    }

    protected T fairSelect(Node root) {
        int max = root.weight;
        int randomWeight = random.nextInt(max);
        Node selectNode = searchNode(root, randomWeight);
        return (T)selectNode.server;
    }

    protected Node searchNode(Node parent, int weight) {

        if (parent.left == null) {
            return parent;
        }

        if (parent.right == null) {
            return parent.left;
        }

        if (parent.left.weight >= weight) {
            return searchNode(parent.left, weight);
        } else {
            return searchNode(parent.right, weight - parent.left.weight);
        }

    }

    /**
     * Update weight of each node of the tree.
     * By create a new tree and insert into the head of the {@link #treeContainer}
     */
    protected void updateWeightTree() {

        log.debug("begin to updateWeightTree...");

        int timeOut = rpcClient.getRpcClientOptions().getReadTimeoutMillis();
        // Create the leaf nodes of the weight tree
        Queue<Node> leafNodes = new LinkedList<Node>();
        if (CollectionUtils.isEmpty(rpcClient.getHealthyInstances())) {
            // if there're no healthy servers, skip create weight, use the random select algorithm instead
            return;
        }

        // the instances to build the weight tree
        List<BrpcChannel> fullWindowInstances = new LinkedList<BrpcChannel>();

        for (BrpcChannel group : rpcClient.getHealthyInstances()) {
            Queue<Integer> window = group.getLatencyWindow();
            // skip instances whose window is not full
            if (window.size() == latencyWindowSize) {
                fullWindowInstances.add(group);
            }
        }

        // some conditions must be satisfied, if not, the fair strategy will not work and use random strategy instead
        if (fullWindowInstances.size() < minInstancesNum
                || fullWindowInstances.size() * 1.0 / rpcClient.getHealthyInstances().size() < activeInstancesRatio) {
            treeContainer = new CopyOnWriteArrayList<Node>();
            invalidInstances = new CopyOnWriteArrayList<T>();
            return;
        }

        // begin to build the weight tree
        for (BrpcChannel group : fullWindowInstances) {
            int weight = calculateWeight(group, timeOut);
            leafNodes.add(new Node(group.hashCode(), weight, true, group));
        }

        // Now begin to create a new weight tree
        Node root = generateWeightTreeByLeafNodes(leafNodes);

        // Insert the new tree into the head of the container
        treeContainer.add(0, root);
        while (treeContainer.size() > 1) {
            // Remove the old weight tree
            treeContainer.remove(1);
        }

        // Since the weight tree has updated by healthy instances, we need to update invalid instances too.
        // Although there maybe new invalid instances added while updating the weight tree, for simplicity,
        // we just remove all invalid instances, at least brpc-java has the retry feature.
        invalidInstances = new CopyOnWriteArrayList<T>();
    }

    /**
     * Calculate the weight of a rpc server
     *
     * @param group   The BrpcChannel instance of a rpc server
     * @param timeOut Read timeout in millis
     * @return Weight num
     */
    protected int calculateWeight(BrpcChannel group, int timeOut) {
        Queue<Integer> window = group.getLatencyWindow();
        int avgLatency = 0;
        for (int latency : window) {
            avgLatency += latency;
        }
        // calculate the average latency
        avgLatency = avgLatency / window.size();
        // normalization to 1-100, to prevent inaccurate calculation of timer, plus a 10ms to the timeout num
        avgLatency = avgLatency * 100 / (timeOut + 10);
        // calculate the final weight
        int weight = 100 - avgLatency;
        return weight > 0 ? weight : 1;
    }

    /**
     * generate the tree by leaf nodes
     * the leaf nodes are the real rpc servers
     * the parent nodes used to calculate the sum of it's children's weight
     *
     * @param leafNodes leaf nodes list
     * @return the root node of the tree
     */
    protected Node generateWeightTreeByLeafNodes(Queue<Node> leafNodes) {

        Queue<Node> nodes = new LinkedList<Node>(leafNodes);
        if (leafNodes.size() % 2 == 1) {
            nodes.add(Node.none);
        }

        Node root = new Node();

        while (nodes.size() > 0) {

            Node left = nodes.poll();
            Node right = nodes.poll();

            if (!left.isLeaf && right == null) {
                root = left;
                break;
            }

            Node parent = new Node(0, 0, false);
            parent.left = left;
            left.parent = parent;

            if (right != null && right != Node.none) {
                parent.right = right;
                parent.weight = left.weight + right.weight;
                right.parent = parent;
            } else {
                parent.weight = left.weight;
            }

            nodes.add(parent);
        }

        return root;
    }

    /**
     * The weight tree node
     */
    static class Node<T> {
        // empty node
        static Node none = new Node();
        int serverId;
        int weight;
        boolean isLeaf;
        Node parent;
        Node left;
        Node right;
        T server;

        public Node() {
        }

        public Node(int serverId, int weight, boolean isLeaf) {
            this.serverId = serverId;
            this.weight = weight;
            this.isLeaf = isLeaf;
        }

        public Node(int serverId, int weight, boolean isLeaf, T server) {
            this.serverId = serverId;
            this.weight = weight;
            this.isLeaf = isLeaf;
            this.server = server;
        }
    }

}
