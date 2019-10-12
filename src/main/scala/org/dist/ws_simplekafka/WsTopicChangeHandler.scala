package org.dist.ws_simplekafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.simplekafka.{PartitionReplicas, ZookeeperClient}
import scala.jdk.CollectionConverters._

class WsTopicChangeHandler(zookeeperClient:WSZookeeperClientImpl, onTopicChange:(String, Seq[PartitionReplicas]) => Unit) extends IZkChildListener{
  var availableTopics: Int = 0;

  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    availableTopics += 1;
    print("Broker Change listener: "+ currentChilds)
      currentChilds.asScala.foreach(topicName => {
        val replicas: Seq[PartitionReplicas] = zookeeperClient.getPartitionAssignmentsFor(topicName)
        onTopicChange(topicName, replicas)
      })
  }
}
