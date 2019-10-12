package org.dist.ws_simplekafka

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.util.Networks

class WsControllerTest extends ZookeeperTestHarness{

  test("Should elect leader") {
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: WSZookeeperClientImpl = new WSZookeeperClientImpl(config1)

    zookeeperClient.registerBroker(Broker(1, config1.hostName, config1.port))
    zookeeperClient.registerBroker(Broker(2, config1.hostName, config1.port))

    val wsController1: WsController = new WsController(zookeeperClient,1);
    val wsController2 = new WsController(zookeeperClient,2);

    wsController1.elect();
    assert(wsController2.elect() == "1");
  }
}
