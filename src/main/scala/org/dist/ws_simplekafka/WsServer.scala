package org.dist.ws_simplekafka

import org.dist.queue.common.Logging
import org.dist.queue.server.Config

class WsServer(val config:Config, val zookeeperClient: WSZookeeperClientImpl, val controller:WsController, val socketServer: WsSimpleSocketServer) extends Logging {
  def startup() = {
    socketServer.startup()
    zookeeperClient.registerSelf()
    controller.elect()

    info(s"Server ${config.brokerId} started with log dir ${config.logDirs}")

  }
}
