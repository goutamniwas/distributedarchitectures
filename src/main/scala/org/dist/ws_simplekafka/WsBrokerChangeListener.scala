package org.dist.ws_simplekafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.common.Logging
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{Controller, ZookeeperClient}

class WsBrokerChangeListener(controller:WsController, zookeeperClient:WSZookeeperClientImpl) extends IZkChildListener with Logging{


  import scala.jdk.CollectionConverters._

  override def handleChildChange(parentPath: String, currentBrokerList: util.List[String]): Unit = {
    try {

      val curBrokerIds = currentBrokerList.asScala.map(_.toInt).toSet
      val newBrokerIds = curBrokerIds -- controller.liveBrokers.map(broker  => broker.id)
      val newBrokers = newBrokerIds.map(zookeeperClient.getBrokerInfo(_))

      newBrokers.foreach(controller.addBroker(_))

//      if (newBrokerIds.size > 0)
//        controller.onBrokerStartup(newBrokerIds.toSeq)

    } catch {
      case e: Throwable => error("Error while handling broker changes", e)
    }
  }
}
