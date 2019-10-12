package org.dist.ws_simplekafka

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.{Controller, ReplicaManager, Server, SimpleConsumer, SimpleKafkaApi, SimpleProducer, SimpleSocketServer, ZookeeperClientImpl}
import org.dist.util.Networks

class WsProducerConsumerTest extends ZookeeperTestHarness{

    test("Should produce ") {
        val wsServer1:WsServer = newBroker(1);
        val wsServer2:WsServer = newBroker(2);
        val wsServer3:WsServer = newBroker(3);

        wsServer1.startup();
        wsServer2.startup();
        wsServer3.startup();

        TestUtils.waitUntilTrue(()⇒ {
            wsServer1.controller.liveBrokers.size == 3
        }, "Waiting for all brokers to be discovered by the controller")


        val wsCreateTopicCommand: WsCreateTopicCommand = new WsCreateTopicCommand(wsServer1.zookeeperClient);
        wsCreateTopicCommand.createTopic("TestTopic",2,2);

        TestUtils.waitUntilTrue(() ⇒ {
            liveBrokersIn(wsServer1) == 3 && liveBrokersIn(wsServer2) == 3 && liveBrokersIn(wsServer3) == 3
        }, "waiting till topic metadata is propogated to all the servers", 2000 )


        val bootstrapBroker = InetAddressAndPort.create(wsServer1.config.hostName, wsServer1.config.port)
        val simpleProducer = new SimpleProducer(bootstrapBroker)
        val firstMsgOffset = simpleProducer.produce("TestTopic", "firstMsg", "message1")
        assert(firstMsgOffset == 1)
       val secMsgOffset = simpleProducer.produce("TestTopic", "secMsg", "message1")
        assert(secMsgOffset == 1)

        val simpleConsumer = new SimpleConsumer(bootstrapBroker)
        val messages = simpleConsumer.consume("TestTopic")

        assert(messages.size() == 2)
        assert(messages.get("firstMsg") == "message1")
        assert(messages.get("secMsg") == "message1")

    }
    private def liveBrokersIn(broker1: WsServer) = {
        broker1.socketServer.kafkaApis.aliveBrokers.size
    }

    private def newBroker(brokerId: Int) = {
        val config = Config(brokerId, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
        val zookeeperClient: WSZookeeperClientImpl = new WSZookeeperClientImpl(config)
        val replicaManager = new WsReplicaManager(config)
        val socketServer1 = new WsSimpleSocketServer(config.brokerId, config.hostName, config.port, new WsSimpleKafkaApi(config, replicaManager))
        val controller = new WsController(zookeeperClient, config.brokerId, socketServer1)
        new WsServer(config, zookeeperClient, controller, socketServer1)
    }

}
