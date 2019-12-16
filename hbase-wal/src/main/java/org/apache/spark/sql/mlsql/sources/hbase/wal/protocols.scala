package org.apache.spark.sql.mlsql.sources.hbase.wal

import org.apache.htrace.fasterxml.jackson.databind.annotation.JsonDeserialize
import tech.mlsql.common.utils.distribute.socket.server.{Request, Response, SocketServerSerDer}


case class NooopsRequest() extends Request[SocketRequest] {
  override def wrap: SocketRequest = SocketRequest(nooopsRequest = this)
}

case class NooopsResponse() extends Response[SocketResponse] {
  override def wrap: SocketResponse = SocketResponse(nooopsResponse = this)
}

case class RequestOffset() extends Request[SocketRequest] {
  override def wrap: SocketRequest = SocketRequest(requestOffset = this)
}

case class OffsetResponse(@JsonDeserialize(contentAs = classOf[java.lang.Long]) offsets: Map[String, String]) extends Response[SocketResponse] {
  override def wrap: SocketResponse = SocketResponse(offsetResponse = this)
}

case class DataResponse(data: List[String]) extends Response[SocketResponse] {
  override def wrap: SocketResponse = SocketResponse(dataResponse = this)
}

case class RequestData(name: String, startOffset: Long, endOffset: Long) extends Request[SocketRequest] {
  override def wrap: SocketRequest = SocketRequest(requestData = this)
}

case class ShutDownServer() extends Request[SocketRequest] {
  override def wrap: SocketRequest = SocketRequest(shutDownServer = this)
}


case class SocketResponse(nooopsResponse: NooopsResponse = null,
                          offsetResponse: OffsetResponse = null,
                          dataResponse: DataResponse = null) {
  def unwrap: Response[_] = {
    if (nooopsResponse != null) nooopsResponse
    else if (offsetResponse != null) offsetResponse
    else if (dataResponse != null) dataResponse
    else null
  }
}

case class SocketRequest(
                          nooopsRequest: NooopsRequest = null,
                          shutDownServer: ShutDownServer = null,
                          requestOffset: RequestOffset = null,
                          requestData: RequestData = null

                        ) {
  def unwrap: Request[_] = {
    if (nooopsRequest != null) {
      nooopsRequest
    } else if (shutDownServer != null) {
      shutDownServer
    } else if (requestOffset != null) requestOffset
    else if (requestData != null) requestData
    else {
      null
    }
  }
}

class SocketClient extends SocketServerSerDer[SocketRequest, SocketResponse]
