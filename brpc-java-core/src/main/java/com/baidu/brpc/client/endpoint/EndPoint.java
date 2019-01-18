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

package com.baidu.brpc.client.endpoint;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Created by wenweihu86 on 2017/5/17.
 */
public class EndPoint {
    String ip;
    int port;

    public EndPoint(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    /**
     * @param hostPort format like "127.0.0.1:8002"
     * @return {@link EndPoint}
     */
    public static EndPoint parseFrom(String hostPort) {
        Validate.notEmpty(hostPort);
        String[] splits = hostPort.split(":");
        EndPoint endPoint = new EndPoint(splits[0], Integer.valueOf(splits[1]));
        return endPoint;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(ip)
                .append(port)
                .toHashCode();
    }

    @Override
    public boolean equals(Object object) {
        boolean flag = false;
        if (object != null && EndPoint.class.isAssignableFrom(object.getClass())) {
            EndPoint rhs = (EndPoint) object;
            flag = new EqualsBuilder()
                    .append(ip, rhs.ip)
                    .append(port, rhs.port)
                    .isEquals();
        }
        return flag;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ip).append(":").append(port);
        return sb.toString();
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

}
