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

package com.baidu.brpc.naming.consul.model;

public class ConsulResponse<T> {

    /**
     * consul返回的具体结果
     */
    private T value;

    private Long consulIndex;

    private Boolean consulKnownLeader;

    private Long consulLastContact;

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public Long getConsulIndex() {
        return consulIndex;
    }

    public void setConsulIndex(Long consulIndex) {
        this.consulIndex = consulIndex;
    }

    public Boolean getConsulKnownLeader() {
        return consulKnownLeader;
    }

    public void setConsulKnownLeader(Boolean consulKnownLeader) {
        this.consulKnownLeader = consulKnownLeader;
    }

    public Long getConsulLastContact() {
        return consulLastContact;
    }

    public void setConsulLastContact(Long consulLastContact) {
        this.consulLastContact = consulLastContact;
    }


}