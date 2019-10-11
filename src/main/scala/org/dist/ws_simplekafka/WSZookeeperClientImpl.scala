package org.dist.ws_simplekafka

import com.google.common.annotations.VisibleForTesting
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.dist.kvstore.JsonSerDes
import org.dist.queue.server.Config
import org.dist.queue.utils.{ZKStringSerializer, ZkUtils}
import org.dist.queue.utils.ZkUtils.Broker
import scala.jdk.CollectionConverters._
import org.dist.simplekafka.ZookeeperClient

class WSZookeeperClientImpl(config: Config)   {

  private val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer);
  val BrokerTopicsPath = "/brokers/topics"
  val BrokerIdsPath = "/brokers/ids"
  val ControllerPath = "/controller"

  private def getBrokerPath(id:Int) = {
    BrokerIdsPath + "/" + id
  }

  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  def createEphemeralPath(zkClient: ZkClient, brokerPath: String, brokerData: String) = {
    try {
      zkClient.createEphemeral(brokerPath, brokerData)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(zkClient, brokerPath)
        zkClient.createEphemeral(brokerPath, brokerData)
      }
    }
  }

  def getBrokerInfo(brokerId: Int): Broker = {
    val data:String = zkClient.readData(getBrokerPath(brokerId))
    JsonSerDes.deserialize(data.getBytes, classOf[Broker])
  }

  @VisibleForTesting
  def registerBroker(broker: Broker) = {
    val brokerData = JsonSerDes.serialize(broker)
    val brokerPath = getBrokerPath(broker.id)
    createEphemeralPath(zkClient, brokerPath, brokerData)
  }

  def subscribeBrokerChangeListener(listener: IZkChildListener): Unit = {
    val result = zkClient.subscribeChildChanges(BrokerIdsPath, listener)
    Option(result).map(_.asScala.toList)
  }
}

