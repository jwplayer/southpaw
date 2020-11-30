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

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.File;
import java.io.IOException;
import java.util.*;


public class KafkaTestServer {
    public static final String HOST = "localhost";

    private final KafkaServerStartable kafkaServer;
    private final AdminClient adminClient;
    private final Integer port;
    private final TestingServer zkServer;

    public KafkaTestServer() {
        try {
            zkServer = new TestingServer();
        } catch (Exception ex) {
            throw new RuntimeException("Couldn't start test ZK server", ex);
        }
        port = InstanceSpec.getRandomPort();
        File logDir;
        logDir = new File(System.getProperty("java.io.tmpdir"), "kafka/logs/log-" + port.toString());
        logDir.deleteOnExit();
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(KafkaConfig.BrokerIdProp(), "0");
        kafkaProperties.setProperty(KafkaConfig.ZkConnectProp(), zkServer.getConnectString());
        kafkaProperties.setProperty(KafkaConfig.PortProp(), port.toString());
        kafkaProperties.setProperty(KafkaConfig.LogDirProp(), logDir.getAbsolutePath());
        kafkaProperties.setProperty(KafkaConfig.DefaultReplicationFactorProp(), "1");
        kafkaProperties.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
        kafkaServer = new KafkaServerStartable(new KafkaConfig(kafkaProperties));
        kafkaServer.startup();

        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getConnectionString());
        adminClient = AdminClient.create(properties);
    }

    public void createTopic(String topic, int partitions) {
        adminClient.createTopics(Collections.singletonList(new NewTopic(topic, partitions, (short) 1)));
    }

    public String getConnectionString() {
        return HOST + ":" + port.toString();
    }

    public void shutdown() {
        adminClient.close();
        kafkaServer.shutdown();
        try {
            zkServer.close();
        } catch (IOException ex) {
            throw new RuntimeException("Couldn't shutdown test ZK server", ex);
        }
    }
}
