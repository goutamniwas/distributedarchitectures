package org.dist.ws_simplekafka

import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.annotations.VisibleForTesting
import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{BrokerChangeListener, ControllerExistsException, LeaderAndReplicaRequest, LeaderAndReplicas, PartitionInfo, PartitionReplicas, TopicChangeHandler, UpdateMetadataRequest}

class WsController(zookeeperClient: WSZookeeperClientImpl, brokerId: Int, socketServer: WsSimpleSocketServer) {

  var liveBrokers: Set[Broker] = Set()
  val correlationId = new AtomicInteger(0)


  def elect() = {
    print("Electing")
    val leaderId = s"${brokerId}"
    try {
      zookeeperClient.tryElectingLeader(leaderId)
      onBecomingLeader()
    } catch {
      case e: ControllerExistsException => e.controllerId
    }
  }

  def onBecomingLeader() = {
    liveBrokers = liveBrokers ++ zookeeperClient.getAllBrokers()
    zookeeperClient.subscribeTopicChangeHandler(new WsTopicChangeHandler(zookeeperClient, onTopicChange))
    zookeeperClient.subscribeBrokerChangeListener(new WsBrokerChangeListener(this, zookeeperClient))
    print("Became leader")
  }

  def onTopicChange(topicName: String, partitionReplicas: Seq[PartitionReplicas]) = {
    print("Topic change")
    print(partitionReplicas);
    val leaderAndReplicas: Seq[LeaderAndReplicas] = partitionReplicas.map(p => {
      print("Iterating leader and replicas" + p);
      val leaderBrokerId = p.brokerIds.head //This is where leader for particular partition is selected
      val leaderBroker = getBroker(leaderBrokerId)
      val replicaBrokers = p.brokerIds.map(id ⇒ getBroker(id))
      LeaderAndReplicas(TopicAndPartition(topicName, p.partitionId), PartitionInfo(leaderBroker, replicaBrokers))
    })

    sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(leaderAndReplicas, partitionReplicas)
    sendUpdateMetadataRequestToAllLiveBrokers(leaderAndReplicas)

  }

  private def getBroker(brokerId:Int) = {
    liveBrokers.find(b ⇒ b.id == brokerId).get
  }

  private def sendUpdateMetadataRequestToAllLiveBrokers(leaderAndReplicas: Seq[LeaderAndReplicas]) = {val brokerListToIsrRequestMap =
    liveBrokers.foreach(broker ⇒ {
      val updateMetadataRequest = UpdateMetadataRequest(liveBrokers.toList, leaderAndReplicas.toList)
      val request = RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest), correlationId.incrementAndGet())
      socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host, broker.port))
    })
  }

  import scala.jdk.CollectionConverters._

  @VisibleForTesting
  def sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(leaderAndReplicas:Seq[LeaderAndReplicas], partitionReplicas: Seq[PartitionReplicas]) = {
    val brokerToLeaderIsrRequest = new util.HashMap[Broker, java.util.List[LeaderAndReplicas]]()
    leaderAndReplicas.foreach(lr ⇒ {
      lr.partitionStateInfo.allReplicas.foreach(broker ⇒ {
        var leaderReplicas = brokerToLeaderIsrRequest.get(broker)
        if (leaderReplicas == null) {
          leaderReplicas = new util.ArrayList[LeaderAndReplicas]()
          brokerToLeaderIsrRequest.put(broker, leaderReplicas)
        }
        leaderReplicas.add(lr)
      })
    })

    val brokers = brokerToLeaderIsrRequest.keySet().asScala
    for(broker ← brokers) {
      val leaderAndReplicas: java.util.List[LeaderAndReplicas] = brokerToLeaderIsrRequest.get(broker)
      val leaderAndReplicaRequest = LeaderAndReplicaRequest(leaderAndReplicas.asScala.toList)
      val request = RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicaRequest), correlationId.getAndIncrement())
      socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host, broker.port))
    }
  }

  def addBroker(broker: Broker) = {
    liveBrokers += broker
  }
}
