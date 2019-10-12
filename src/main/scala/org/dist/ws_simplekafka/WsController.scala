package org.dist.ws_simplekafka

import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{BrokerChangeListener, ControllerExistsException, TopicChangeHandler}

class WsController(zookeeperClient: WSZookeeperClientImpl, brokerId: Int, socketServer: WsSimpleSocketServer) {

  var liveBrokers: Set[Broker] = Set()

  def elect() = {
    val leaderId = s"${brokerId}"
    try {
    zookeeperClient.tryElectingLeader(leaderId)
    } catch {
      case e: ControllerExistsException => e.controllerId
    }
  }

  def onBecomingLeader() = {
    liveBrokers = liveBrokers ++ zookeeperClient.getAllBrokers()
    zookeeperClient.subscribeTopicChangeHandler(new WsTopicChangeHandler(zookeeperClient, onTopicChange))
    zookeeperClient.subscribeBrokerChangeListener(new WsBrokerChangeListener(this, zookeeperClient))
  }

  def onTopicChange(): Unit = {

  }
}
