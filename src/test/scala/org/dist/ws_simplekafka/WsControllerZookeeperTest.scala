package org.dist.ws_simplekafka

import java.util

import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.zookeeper.KeeperException.NoNodeException
import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.api.RequestOrResponse
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.SimpleSocketServer
import org.dist.util.Networks


class WsTestSocketServer(config: Config) extends WsSimpleSocketServer(config.brokerId, config.hostName, config.port, null) {
  var messages = new util.ArrayList[RequestOrResponse]()
  var toAddresses = new util.ArrayList[InetAddressAndPort]()

  override def sendReceiveTcp(message: RequestOrResponse, to: InetAddressAndPort): RequestOrResponse = {
    print("Test");
    this.messages.add(message)
    this.toAddresses.add(to)
    RequestOrResponse(message.requestId, "", message.correlationId)
  }
}

class WsControllerZookeeperTest extends ZookeeperTestHarness{

  test("should register broker") {
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: WSZookeeperClientImpl = new WSZookeeperClientImpl(config1)
    zookeeperClient.registerBroker(Broker(config1.brokerId, config1.hostName, config1.port))
    val actual = zookeeperClient.getBrokerInfo(1)
    print(actual);
    assert(actual != null);
  }

  test("should throw error") {
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: WSZookeeperClientImpl = new WSZookeeperClientImpl(config1)
    zookeeperClient.registerBroker(Broker(config1.brokerId, config1.hostName, config1.port))

    assertThrows[ZkNoNodeException]{
      zookeeperClient.getBrokerInfo(2)
    };
  }

  test("should have three brokers") {
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val config2 = Config(2, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val config3 = Config(3, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))

    val zookeeperClient1: WSZookeeperClientImpl = new WSZookeeperClientImpl(config1)
    val zookeeperClient2: WSZookeeperClientImpl = new WSZookeeperClientImpl(config2)
    val zookeeperClient3: WSZookeeperClientImpl = new WSZookeeperClientImpl(config3)

    zookeeperClient1.registerBroker(Broker(config1.brokerId, config1.hostName, config1.port))
    zookeeperClient2.registerBroker(Broker(config2.brokerId, config2.hostName, config2.port))
    zookeeperClient3.registerBroker(Broker(config3.brokerId, config3.hostName, config3.port))

    val wsTestSocketServer1: WsTestSocketServer = new WsTestSocketServer(config1);
    val wsTestSocketServer2: WsTestSocketServer = new WsTestSocketServer(config2);
    val wsTestSocketServer3: WsTestSocketServer = new WsTestSocketServer(config3);

    val wsController1:WsController = new WsController(zookeeperClient1,1, wsTestSocketServer1)
    val wsController2:WsController = new WsController(zookeeperClient2,2, wsTestSocketServer2)
    val wsController3:WsController = new WsController(zookeeperClient3,3, wsTestSocketServer3)

    wsController1.elect();
    wsController2.elect();
    wsController3.elect();

    val createTopicCommand: WsCreateTopicCommand = new WsCreateTopicCommand(zookeeperClient1);
    createTopicCommand.createTopic("TopicName", 3, 3);

    TestUtils.waitUntilTrue(() => {
      wsTestSocketServer1.messages.size() > 4
    }, "waiting socket message")
    println("\n***"+wsTestSocketServer1.messages)
  }
}