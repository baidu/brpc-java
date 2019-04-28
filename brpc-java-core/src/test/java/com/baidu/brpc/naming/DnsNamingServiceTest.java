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

package com.baidu.brpc.naming;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;

import com.baidu.brpc.client.instance.Endpoint;
import com.baidu.brpc.test.BaseMockitoTest;

public class DnsNamingServiceTest extends BaseMockitoTest {

    @Test
    public void testAll() {
        String serverList = "dns://127.0.0.1:8888";
        DnsNamingService namingService = new DnsNamingService(new BrpcURL(serverList));
        assertThat(namingService.getHostPort(), is("127.0.0.1:8888"));
        List<Endpoint> endPoints = namingService.lookup(null);
        assertThat(endPoints, hasItems(new Endpoint("127.0.0.1", 8888)));
        namingService.unsubscribe(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyServerList() {
        DnsNamingService namingService = new DnsNamingService(new BrpcURL(""));
        namingService.lookup(null);
    }

}