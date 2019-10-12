package org.dist.ws_simplekafka

import org.dist.kvstore.JsonSerDes
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{LeaderAndReplicaRequest, LeaderAndReplicas, PartitionInfo, ProduceRequest, ReplicaManager, SimpleKafkaApi, TopicMetadataRequest, UpdateMetadataRequest}
import org.dist.util.Networks

class WsSimpleKafkaApiTest extends ZookeeperTestHarness{

  test("should create leader and follower replicas") {
    val config = Config(0, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new WsReplicaManager(config)
    val simpleKafkaApi = new WsSimpleKafkaApi(config, replicaManager)
    val leaderAndReplicas = LeaderAndReplicaRequest(List(LeaderAndReplicas(TopicAndPartition("topic1", 0), PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000))))))
    val request = RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicas), 1)
    simpleKafkaApi.handle(request)
    assert(replicaManager.allPartitions.size() == 1)
    val partition = replicaManager.allPartitions.get(TopicAndPartition("topic1", 0))
    assert(partition.logFile.exists())
  }

  test("should update meta data key") {
    val config = Config(0, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new WsReplicaManager(config)
    val simpleKafkaApi = new WsSimpleKafkaApi(config, replicaManager)
    val leaderAndReplicas:List[LeaderAndReplicas] = List(LeaderAndReplicas(TopicAndPartition("topic1", 0), PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000)))));
    val updateMetadataRequest: UpdateMetadataRequest = UpdateMetadataRequest(List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000)),leaderAndReplicas);
    val request = RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest), 1)
    simpleKafkaApi.handle(request)
    print(simpleKafkaApi.leaderCache)
    val leaderAndReplicas2:List[LeaderAndReplicas] = List(LeaderAndReplicas(TopicAndPartition("topic1", 0), PartitionInfo(Broker(1, "10.10.10.11", 8000), List(Broker(1, "10.10.10.11", 8000)))));
    val updateMetadataRequest2: UpdateMetadataRequest = UpdateMetadataRequest(List( Broker(1, "10.10.10.11", 8000)),leaderAndReplicas2);
    val request2 = RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest2), 1)
    simpleKafkaApi.handle(request2)
    var expectedLeaderCache = new java.util.HashMap[TopicAndPartition, PartitionInfo]
    expectedLeaderCache.put(TopicAndPartition("topic1", 0),PartitionInfo(Broker(1, "10.10.10.11", 8000), List( Broker(1, "10.10.10.11", 8000))));
    print(simpleKafkaApi.leaderCache)
    assert(simpleKafkaApi.leaderCache == expectedLeaderCache)
    print(simpleKafkaApi.aliveBrokers)
    assert(simpleKafkaApi.aliveBrokers == List(Broker(1, "10.10.10.11", 8000)))
  }

  test("Get Metadata") {
    val config = Config(0, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new WsReplicaManager(config)

    val simpleKafkaApi = new WsSimpleKafkaApi(config, replicaManager)
    val leaderAndReplicas:List[LeaderAndReplicas] = List(LeaderAndReplicas(TopicAndPartition("topic1", 0), PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000)))));
    val updateMetadataRequest: UpdateMetadataRequest = UpdateMetadataRequest(List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000)),leaderAndReplicas);
    val request = RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest), 1)
    simpleKafkaApi.handle(request)

    val topicMetadataRequest:TopicMetadataRequest = new TopicMetadataRequest("topic1");
    val getRequest = RequestOrResponse(RequestKeys.MetadataKey, JsonSerDes.serialize(topicMetadataRequest), 1)

    assert(simpleKafkaApi.handle(getRequest).messageBodyJson.toString == """{"topicPartitions":{"[topic1,0]":{"leader":{"id":1,"host":"10.10.10.10","port":8000},"allReplicas":[{"id":0,"host":"10.10.10.10","port":8000},{"id":1,"host":"10.10.10.11","port":8000}]}}}""")


  }

  test("Should produce key") {
    val config = Config(0, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new WsReplicaManager(config)
    val simpleKafkaApi = new WsSimpleKafkaApi(config, replicaManager)
    val leaderAndReplicas = LeaderAndReplicaRequest(List(LeaderAndReplicas(TopicAndPartition("topic1", 0), PartitionInfo(Broker(1, "10.10.10.10", 8000), List(Broker(0, "10.10.10.10", 8000), Broker(1, "10.10.10.11", 8000))))))
    val leaderAndReplicaRequest = RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicas), 1)
    simpleKafkaApi.handle(leaderAndReplicaRequest)

    val produceRequest: ProduceRequest = ProduceRequest(TopicAndPartition("topic1", 0), "key", "message");

    val produceKeyRequest = RequestOrResponse(RequestKeys.ProduceKey, JsonSerDes.serialize(produceRequest), 1)
    val actualProduceKeyResponse = simpleKafkaApi.handle(produceKeyRequest);

    assert(actualProduceKeyResponse == RequestOrResponse(0,"""{"offset":1}""",1))
  }
}
