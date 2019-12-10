package org.apache.spark.sql.mlsql.sources.hbase.wal

import tech.mlsql.common.utils.distribute.socket.server.{Request, Response, SocketServerSerDer}


case class NooopsRequest() extends Request[SocketRequest] {
  override def wrap: SocketRequest = SocketRequest(nooopsRequest = this)
}

case class NooopsResponse() extends Response[SocketResponse] {
  override def wrap: SocketResponse = SocketResponse(nooopsResponse = this)
}

case class ShutDownServer() extends Request[SocketRequest] {
  override def wrap: SocketRequest = SocketRequest(shutDownServer = this)
}

case class SocketResponse(nooopsResponse: NooopsResponse = null) {
  def unwrap: Response[_] = {
    if (nooopsResponse != null) nooopsResponse
    else null
  }
}

case class SocketRequest(
                          nooopsRequest: NooopsRequest = null,
                          shutDownServer: ShutDownServer = null

                        ) {
  def unwrap: Request[_] = {
    if (nooopsRequest != null) {
      nooopsRequest
    } else if (shutDownServer != null) {
      shutDownServer
    } else {
      null
    }
  }
}

class SocketClient extends SocketServerSerDer[SocketRequest, SocketResponse]
