/*
 * Copyright 2018 Longtail Ad Solutions (DBA JW Player)
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
package com.jwplayer.southpaw.util;

import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.curator.test.TestingServer;

import java.io.IOException;


public class ZookeeperTestServer {
    private TestingServer testingServer;

    public ZookeeperTestServer() {
        try {
            testingServer = new TestingServer();
        } catch (Exception ex) {
            throw new RuntimeException("Couldn't start test ZK server", ex);
        }
    }

    public String getConnectionString() {
        return testingServer.getConnectString();
    }

    public ZkUtils getZkUtils() {
        ZkClient zkClient = new ZkClient(getConnectionString(), Integer.MAX_VALUE, 10000, ZKStringSerializer$.MODULE$);
        return new ZkUtils(zkClient, new ZkConnection(getConnectionString()), false);
    }

    public void shutdown() {
        try {
            testingServer.close();
        } catch (IOException ex) {
            throw new RuntimeException("Couldn't shutdown test ZK server", ex);
        }
    }
}
