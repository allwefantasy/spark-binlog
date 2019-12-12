package org.apache.spark.sql.mlsql.sources.hbase.wal

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.wal.WAL
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{BinlogWriteAheadLog, RawEvent}
import tech.mlsql.common.utils.distribute.socket.server.SocketServerInExecutor
import tech.mlsql.common.utils.network.NetUtils

import scala.collection.mutable.ArrayBuffer

/**
 * 9/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class HBaseWALSocketServerInExecutor[T](taskContextRef: AtomicReference[T], checkpointDir: String,
                                        hadoopConf: Configuration, isWriteAheadStorage: Boolean = true)
  extends SocketServerInExecutor[T](taskContextRef, "hbase-wal-socket-server-in-executor") with Logging {

  @volatile private var markClose: AtomicBoolean = new AtomicBoolean(false)
  private val connections = new ArrayBuffer[Socket]()
  val client = new SocketClient()

  var walLogPath: String = ""
  var startTime: Long = 0



  private val aheadLogBuffer = new java.util.concurrent.ConcurrentLinkedDeque[RawEvent]()


  private val writeAheadLog = {
    val sparkEnv = SparkEnv.get
    val tmp = new BinlogWriteAheadLog(UUID.randomUUID().toString, sparkEnv.serializerManager, sparkEnv.conf, hadoopConf, checkpointDir)
    tmp.cleanupOldBlocks(System.currentTimeMillis(), true)
    tmp
  }


  override def close() = {
    // make sure we only close once
    if (markClose.compareAndSet(false, true)) {
      logInfo(s"Shutdown ${host}. This may caused by the task is killed.")
    }
  }

  def isClosed = markClose.get()

  override def handleConnection(socket: Socket): Unit = {
    connections += socket
    socket.setKeepAlive(true)
    val dIn = new DataInputStream(socket.getInputStream)
    val dOut = new DataOutputStream(socket.getOutputStream)

    while (true) {
      client.readRequest(dIn) match {
        case _: NooopsRequest =>
          client.sendResponse(dOut, NooopsResponse())
        case requestOffset: RequestOffset =>
        case requestData: RequestData =>

      }
    }
  }

  override def host: String = {
    if (SparkEnv.get == null) {
      //When SparkEnv.get is null, the program may run in a test
      //So return local address would be ok.
      "127.0.0.1"
    } else {
      val hostName = tech.mlsql.common.utils.network.SparkExecutorInfo.getInstance.hostname
      if (hostName == null) NetUtils.getHost else hostName
    }
  }

  def format_throwable(e: Throwable, skipPrefix: Boolean = false) = {
    (e.toString.split("\n") ++ e.getStackTrace.map(f => f.toString)).map(f => f).toSeq.mkString("\n")
  }


  def format_full_exception(buffer: ArrayBuffer[String], e: Exception, skipPrefix: Boolean = true) = {
    var cause = e.asInstanceOf[Throwable]
    buffer += format_throwable(cause, skipPrefix)
    while (cause.getCause != null) {
      cause = cause.getCause
      buffer += "caused by：\n" + format_throwable(cause, skipPrefix)
    }

  }

  private var connectThread: Thread = null

  def connectWAL() = {
    connectThread = new Thread(s"connect hbase wal in ${walLogPath} ") {
      setDaemon(true)

      override def run(): Unit = {
        try {
          val hbaseWALclient = new HBaseWALClient(walLogPath, startTime, hadoopConf)
          hbaseWALclient.register(new HBaseWALEventListener {
            override def onEvent(event: WAL.Entry): Unit = {

            }
          })
          hbaseWALclient.connect()

        } catch {
          case e: Exception =>
            throw e
        }

      }
    }
    connectThread.start()

  }
}


