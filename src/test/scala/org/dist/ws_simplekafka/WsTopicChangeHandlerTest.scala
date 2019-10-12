package org.dist.ws_simplekafka

import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks

class WsTopicChangeHandlerTest extends ZookeeperTestHarness {

    test("should listen to broker addition") {
      val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
      val zookeeperClient: WSZookeeperClientImpl = new WSZookeeperClientImpl(config1)
      val topicChangeHandler: WsTopicChangeHandler = new WsTopicChangeHandler();

      val wsCreateTopicCommand: WsCreateTopicCommand = new WsCreateTopicCommand(zookeeperClient);

      zookeeperClient.registerBroker(Broker(1, config1.hostName, config1.port))
      zookeeperClient.registerBroker(Broker(2, config1.hostName, config1.port))
      zookeeperClient.registerBroker(Broker(3, config1.hostName, config1.port))

      zookeeperClient.subscribeTopicChangeHandler(topicChangeHandler);

      wsCreateTopicCommand.createTopic("Test",3,2);
      wsCreateTopicCommand.createTopic("Test1",4,3)


      TestUtils.waitUntilTrue(() => {
        topicChangeHandler.availableTopics == 2
      } , "Testing available topics");
    }
}
