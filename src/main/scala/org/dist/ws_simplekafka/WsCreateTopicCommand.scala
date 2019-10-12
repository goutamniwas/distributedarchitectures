package org.dist.ws_simplekafka

import java.util.Random

import org.dist.simplekafka.{PartitionReplicas, ZookeeperClient}

import scala.collection.mutable

class WsCreateTopicCommand(zookeeperClient:WSZookeeperClientImpl) {

  val rand = new Random
  def assignReplicasToBrokers(brokerList: List[Int], nPartitions: Int, replicationFactor: Int) = {

    val ret = new mutable.HashMap[Int, List[Int]]()
    val startIndex = rand.nextInt(brokerList.size)
    var currentPartitionId = 0

    var nextReplicaShift = rand.nextInt(brokerList.size)
    for (partitionId <- 0 until nPartitions) {
      if (currentPartitionId > 0 && (currentPartitionId % brokerList.size == 0))
        nextReplicaShift += 1
      val firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size
      var replicaList = List(brokerList(firstReplicaIndex))
      for (j <- 0 until replicationFactor - 1)
        replicaList ::= brokerList(getWrappedIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size))
      ret.put(currentPartitionId, replicaList.reverse)
      currentPartitionId = currentPartitionId + 1
    }
    val partitionIds = ret.toMap.keySet
    partitionIds.map(id => PartitionReplicas(id, ret(id)))
  }


  private def getWrappedIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }

  def createTopic(topicName: String, partitions: Int, replicationFactor: Int): Any = {
    val brokerIds = zookeeperClient.getAllBrokerIds()
    //get list of brokers
    //assign replicas to partition
    val partitionReplicas = assignReplicasToBrokers(brokerIds.toList, partitions, replicationFactor)
    // register topic with partition assignments to zookeeper
    zookeeperClient.setPartitionReplicasForTopic(topicName, partitionReplicas)
  }


}
