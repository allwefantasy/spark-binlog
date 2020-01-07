package org.apache.spark.sql.mlsql.sources.hbase.wal

import java.io.{DataInputStream, DataOutputStream}
import java.util
import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkEnv
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset}
import org.apache.spark.sql.mlsql.sources.hbase.wal.io.{DeleteWriter, PutWriter}
import org.apache.spark.streaming.RawEvent
import tech.mlsql.binlog.common.OriginalSourceServerInExecutor
import tech.mlsql.common.utils.distribute.socket.server.SocketIteratorMark
import tech.mlsql.common.utils.network.NetUtils

import scala.collection.JavaConverters._

/**
 * 9/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class HBaseWALSocketServerInExecutor[T](taskContextRef: AtomicReference[T], checkpointDir: String,
                                        hadoopConf: Configuration, isWriteAheadStorage: Boolean = true) extends OriginalSourceServerInExecutor[T](taskContextRef: AtomicReference[T], checkpointDir: String,
  hadoopConf: Configuration, isWriteAheadStorage: Boolean) {

  private val client = new SocketClient()
  private var originSourceClient: HBaseWALClient = null
  private var walLogPath: String = ""
  private var oldWALLogPath: String = ""
  private var startTime: Long = 0L

  def setOldWALLogPath(oldWALLogPath: String) = this.oldWALLogPath = oldWALLogPath

  def setWalLogPath(walLogPath: String) = this.walLogPath = walLogPath

  def setStartTime(startTime: Long) = this.startTime = startTime

  def setDatabaseNamePattern(databaseNamePattern: Option[Pattern]) = this.databaseNamePattern = databaseNamePattern

  def setTableNamePattern(tableNamePattern: Option[Pattern]) = this.tableNamePattern = tableNamePattern

  override def connect: Unit = {
    assert(walLogPath != "", "walLogPath is required")
    assert(startTime != 0, "startTime is required")
    connectWAL(walLogPath, startTime)
  }

  override def pause: Unit = {
    throw new RuntimeException("not support")
  }

  override def resume: Unit = {
    throw new RuntimeException("not support")
  }

  override def closeOriginalSource: Unit = {
    if (originSourceClient != null) {
      originSourceClient.disConnect
    }
  }


  override def flushAheadLog: Unit = {
    super.flushAheadLog
  }

  private def toOffset(rawBinlogEvent: RawEvent) = {
    rawBinlogEvent.asInstanceOf[RawHBaseWALEventsSerialization].pos().asInstanceOf[LongOffset].offset
  }

  val putWriter = new PutWriter()
  val delWriter = new DeleteWriter()

  def convertRawBinlogEventRecord(event: RawHBaseWALEvent) = {
    val writer = if (event.put != null) putWriter else delWriter
    val jsonList = try {
      writer.writeEvent(event)
    } catch {
      case e: Exception =>
        logError("", e)
        new util.ArrayList[String]()
    }
    jsonList
  }

  override def process(dIn: DataInputStream, dOut: DataOutputStream): Unit = {
    client.readRequest(dIn) match {
      case _: NooopsRequest =>
        client.sendResponse(dOut, NooopsResponse())
      case _: ShutDownServer => close()
      case _: RequestOffset =>
        synchronized {
          flushAheadLog
        }
        val offsets = committedOffsets.asScala.
          map(f => (f._1, (LongOffset.convert(f._2).get.offset+1).toString))
        client.sendResponse(dOut, OffsetResponse(offsets.toMap))
      case RequestData(name, startOffset, endOffset) =>
        try {
          if (isWriteAheadStorage) {
            client.sendMarkRequest(dOut, SocketIteratorMark.HEAD)
            writeAheadLogMap.get(name).read((records) => {
              records.foreach { record =>
                if (toOffset(record) >= startOffset && toOffset(record) < endOffset) {
                  client.sendResponse(dOut,
                    DataResponse(record.asInstanceOf[RawHBaseWALEventsSerialization].item.toList))
                }
              }
            })
            client.sendMarkRequest(dOut, SocketIteratorMark.END)
          } else {
            client.sendMarkRequest(dOut, SocketIteratorMark.HEAD)
            var item = queue.poll()
            while (item != null && toOffset(item) >= startOffset && toOffset(item) < endOffset) {
              client.sendResponse(dOut, DataResponse(item.asInstanceOf[RawHBaseWALEventsSerialization].item.toList))
              item = queue.poll()
              currentQueueSize.decrementAndGet()
            }
            client.sendMarkRequest(dOut, SocketIteratorMark.END)
          }

        } catch {
          case e: Exception =>
            logError("", e)
        }


    }
  }

  def matchTable(item: RawHBaseWALEvent) = {
    (databaseNamePattern
      .map(_.matcher(item.db).matches())
      .getOrElse(false) && tableNamePattern
      .map(_.matcher(item.table).matches())
      .getOrElse(false))
  }

  def connectWAL(walLogPath: String, startTime: Long) = {
    connectThread = new Thread(s"connect hbase wal in ${walLogPath} ") {
      setDaemon(true)

      override def run(): Unit = {
        try {
          val hbaseWALclient = new HBaseWALClient(walLogPath, oldWALLogPath, startTime, hadoopConf)
          hbaseWALclient.register(new HBaseWALEventListener {
            override def onEvent(_event: Seq[RawHBaseWALEvent]): Unit = {
              val event = _event.filter(matchTable(_))
              if (event.size > 0) {
                val newEvt = RawHBaseWALEventsSerialization(event.head.key(), event.head.pos(), event.map(f => convertRawBinlogEventRecord(f)).flatMap(f => f.asScala).toList)
                addRecord(newEvt)
              }
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

  override def less(a: Offset, b: Offset): Boolean = {
    require(a != null || b != null, "two offsets should not be null at the same time ")
    if (a == null) true
    else if (b == null) false
    else LongOffset.convert(a).get.offset < LongOffset.convert(b).get.offset
  }
}


