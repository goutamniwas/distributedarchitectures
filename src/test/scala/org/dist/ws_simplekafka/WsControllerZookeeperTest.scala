package org.dist.ws_simplekafka

import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.zookeeper.KeeperException.NoNodeException
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.util.Networks

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

}