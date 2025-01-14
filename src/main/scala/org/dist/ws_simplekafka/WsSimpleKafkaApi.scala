package org.dist.ws_simplekafka

import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker

import scala.jdk.CollectionConverters._
import org.dist.simplekafka.{ConsumeRequest, ConsumeResponse, LeaderAndReplicaRequest, PartitionInfo, ProduceRequest, ProduceResponse, TopicMetadataRequest, TopicMetadataResponse, UpdateMetadataRequest}

class WsSimpleKafkaApi(config: Config, replicaManager: WsReplicaManager) {
  var aliveBrokers = List[Broker]()
  var leaderCache = new java.util.HashMap[TopicAndPartition, PartitionInfo]

  def handle(request: RequestOrResponse): RequestOrResponse = {
    request.requestId match {
      case RequestKeys.LeaderAndIsrKey => {
        val leaderAndReplicasRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[LeaderAndReplicaRequest])
        leaderAndReplicasRequest.leaderReplicas.foreach(leaderAndReplicas ⇒ {
          val topicAndPartition = leaderAndReplicas.topicPartition
          val leader = leaderAndReplicas.partitionStateInfo.leader
          if (leader == config.brokerId)
            replicaManager.makeLeader(topicAndPartition)
          else
            replicaManager.makeFollower(topicAndPartition, leader.id)
        })
        RequestOrResponse(RequestKeys.LeaderAndIsrKey, "", request.correlationId)
      }
      case RequestKeys.UpdateMetadataKey ⇒ {
        val updateMetadataRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[UpdateMetadataRequest])
        aliveBrokers = updateMetadataRequest.aliveBrokers
        updateMetadataRequest.leaderReplicas.foreach(leaderReplica => {
          leaderCache.put(leaderReplica.topicPartition, leaderReplica.partitionStateInfo)
        })
        RequestOrResponse(RequestKeys.UpdateMetadataKey, "", request.correlationId)
      }
      case RequestKeys.MetadataKey ⇒ {
        val topicMetadataRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[TopicMetadataRequest])
        val topicAndPartitions = leaderCache.keySet().asScala.filter(topicAndPartition ⇒ topicAndPartition.topic == topicMetadataRequest.topicName)
        val partitionInfo: Map[TopicAndPartition, PartitionInfo] = topicAndPartitions.map((tp: TopicAndPartition) ⇒ {
          (tp, leaderCache.get(tp))
        }).toMap
        val topicMetadata = TopicMetadataResponse(partitionInfo)
        RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(topicMetadata), request.correlationId)
      }
      case RequestKeys.ProduceKey ⇒ {
        val produceRequest: ProduceRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[ProduceRequest])
        val partition = replicaManager.getPartition(produceRequest.topicAndPartition)
        val offset = partition.writer.append(produceRequest.key, produceRequest.message.getBytes)
        RequestOrResponse(RequestKeys.ProduceKey, JsonSerDes.serialize(ProduceResponse(offset)), request.correlationId)
      }
      case RequestKeys.FetchKey ⇒ {
        val consumeRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[ConsumeRequest])
        val partition = replicaManager.getPartition(consumeRequest.topicAndPartition)
        val rows = partition.read(consumeRequest.offset)
        val consumeResponse = ConsumeResponse(rows.map(row ⇒ (row.key, row.value)).toMap)
        RequestOrResponse(RequestKeys.FetchKey, JsonSerDes.serialize(consumeResponse), request.correlationId)
      }
    }
  }
}
