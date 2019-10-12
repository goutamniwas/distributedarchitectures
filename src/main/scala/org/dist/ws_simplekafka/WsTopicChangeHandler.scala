package org.dist.ws_simplekafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.simplekafka.{PartitionReplicas, ZookeeperClient}

class WsTopicChangeHandler(zookeeperClient:WSZookeeperClientImpl, onTopicChange:(String, Seq[PartitionReplicas])) extends IZkChildListener{
  var availableTopics: Int = 0;

  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    availableTopics += 1;
    print("Broker Change listener: "+ currentChilds)
  }
}
