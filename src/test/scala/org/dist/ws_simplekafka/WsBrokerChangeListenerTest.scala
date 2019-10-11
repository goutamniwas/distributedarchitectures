package org.dist.ws_simplekafka
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.util.Networks

class WsBrokerChangeListenerTest extends ZookeeperTestHarness {

    test("should listen to broker addition") {
      val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
      val zookeeperClient: WSZookeeperClientImpl = new WSZookeeperClientImpl(config1)
      val brokerChangeListener: WsBrokerChangeListener = new WsBrokerChangeListener();
      zookeeperClient.subscribeBrokerChangeListener(brokerChangeListener);

      zookeeperClient.registerBroker(Broker(1, config1.hostName, config1.port))
      zookeeperClient.registerBroker(Broker(2, config1.hostName, config1.port))
      zookeeperClient.registerBroker(Broker(3, config1.hostName, config1.port))

      TestUtils.waitUntilTrue(() => {
        brokerChangeListener.liveBrokers == 3
      } , "Testing live broker count");
    }
}
