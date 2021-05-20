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

import java.io.File;
import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;
import org.apache.curator.test.InstanceSpec;


public class KafkaTestServer {
  public static final String HOST = "localhost";

  private KafkaServerStartable kafkaServer;
  private Integer port;
  private ZookeeperTestServer zkServer;
  private ZkUtils zkUtils;

  public KafkaTestServer() {
    zkServer = new ZookeeperTestServer();
    zkUtils = zkServer.getZkUtils();
    port = InstanceSpec.getRandomPort();
    File logDir;
    logDir = new File(System.getProperty("java.io.tmpdir"), "kafka/logs/log-" + port.toString());
    logDir.deleteOnExit();
    Properties kafkaProperties = new Properties();
    kafkaProperties.setProperty(KafkaConfig.BrokerIdProp(), "0");
    kafkaProperties.setProperty(KafkaConfig.ZkConnectProp(), zkServer.getConnectionString());
    kafkaProperties.setProperty(KafkaConfig.PortProp(), port.toString());
    kafkaProperties.setProperty(KafkaConfig.LogDirProp(), logDir.getAbsolutePath());
    kafkaProperties.setProperty(KafkaConfig.DefaultReplicationFactorProp(), "1");
    kafkaProperties.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
    kafkaServer = new KafkaServerStartable(new KafkaConfig(kafkaProperties));
    kafkaServer.startup();
  }

  public void createTopic(String topic, Integer partitions) {
    if (!AdminUtils.topicExists(zkUtils, topic)) {
      AdminUtils.createTopic(zkUtils, topic, partitions, 1, new Properties(), null);
    }
  }

  public String getConnectionString() {
    return HOST + ":" + port.toString();
  }

  public void shutdown() {
    kafkaServer.shutdown();
    zkServer.shutdown();
  }
}
