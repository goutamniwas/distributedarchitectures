package org.dist.ws_simplekafka

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.PartitionReplicas
import org.dist.util.Networks
import org.scalatest.FunSuite

class WsCreateTopicCommandTest extends ZookeeperTestHarness{

  test("should create topic")  {

    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: WSZookeeperClientImpl = new WSZookeeperClientImpl(config1)
    zookeeperClient.registerBroker(Broker(config1.brokerId, config1.hostName, config1.port))
    zookeeperClient.registerBroker(Broker(2, config1.hostName, config1.port))
    val createTopicCommand: WsCreateTopicCommand = new WsCreateTopicCommand(zookeeperClient);
    createTopicCommand.createTopic("TestTopic",3,3);
    assert(zookeeperClient.getAllTopics().contains("TestTopic"));
  }

  test("should assign replication factor") {
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: WSZookeeperClientImpl = new WSZookeeperClientImpl(config1)
    zookeeperClient.registerBroker(Broker(config1.brokerId, config1.hostName, config1.port))
    zookeeperClient.registerBroker(Broker(2, config1.hostName, config1.port))
    zookeeperClient.registerBroker(Broker(3, config1.hostName, config1.port))
    val createTopicCommand: WsCreateTopicCommand = new WsCreateTopicCommand(zookeeperClient);
    val partitionReplicas: Set[PartitionReplicas] = createTopicCommand.assignReplicasToBrokers(zookeeperClient.getAllBrokerIds().toList,3,3);
    assert(partitionReplicas.find(p => {
      p.brokerIds.size != 3
    }) == None);

  }

}
