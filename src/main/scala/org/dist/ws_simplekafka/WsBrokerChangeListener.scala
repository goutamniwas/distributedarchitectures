package org.dist.ws_simplekafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{Controller, ZookeeperClient}

class WsBrokerChangeListener(controller:WsController, zookeeperClient:WSZookeeperClientImpl) extends IZkChildListener {


  var liveBrokers: Int = 0;

  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    liveBrokers += 1;
    print("Broker Change listener: "+ currentChilds)
  }
}
