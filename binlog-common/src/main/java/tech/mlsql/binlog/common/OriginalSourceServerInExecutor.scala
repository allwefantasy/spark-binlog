package tech.mlsql.binlog.common

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.Offset
import org.apache.spark.streaming.{BinlogWriteAheadLog, RawEvent}
import tech.mlsql.common.utils.distribute.socket.server.SocketServerInExecutor

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * 12/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
abstract class OriginalSourceServerInExecutor[T](taskContextRef: AtomicReference[T], checkpointDir: String,
                                                 hadoopConf: Configuration, isWriteAheadStorage: Boolean = true)
  extends SocketServerInExecutor[T](taskContextRef, "hbase-wal-socket-server-in-executor") with Logging {
  protected var connectThread: Thread = null

  protected val queue = new util.ArrayDeque[RawEvent]()

  protected val writeAheadLogMap = new ConcurrentHashMap[String, BinlogWriteAheadLog]()


  protected def createWriteAheadLog(name: String) = {
    val sparkEnv = SparkEnv.get
    val tmp = new BinlogWriteAheadLog(name, sparkEnv.serializerManager, sparkEnv.conf, hadoopConf, checkpointDir)
    tmp.cleanupOldBlocks(System.currentTimeMillis(), true)
    if (writeAheadLogMap.containsKey(name)) {
      throw new RuntimeException(s"createWriteAheadLog ${name} for twice")
    }
    writeAheadLogMap.put(name, tmp)
    tmp
  }

  protected var databaseNamePattern: Option[Pattern] = None
  protected var tableNamePattern: Option[Pattern] = None

  protected var maxBinlogQueueSize: Long = 0L


  protected val aheadLogBuffer = new java.util.concurrent.ConcurrentLinkedDeque[RawEvent]()
  protected var aheadLogBufferFlushSize = 100000L
  protected var aheadLogSaveTime = 1000 * 60 * 60 * 24 * 3L


  protected val committedOffsets = {
    // recover from hdfs

    val res = new ConcurrentHashMap[String, Offset]()

    val path = new Path(checkpointDir, new Path("receivedData"))
    val hdfsContext = new HDFSContext(path, hadoopConf)
    if (hdfsContext.fs.exists(path)) {
      val logFileNames = hdfsContext.fc.listStatus(path)
      while (logFileNames.hasNext) {
        val name = logFileNames.next()
        val aheadLog = createWriteAheadLog(name.getPath.getName)
        aheadLog.read(events => {
          events.foreach { event =>
            if (!res.containsKey(event.key) || less(res.get(event.key()), event.pos())) {
              res.put(event.key(), event.pos())
            }
          }
        })
      }
    }
    res

  }
  protected val uncommittedOffsets = {
    val res = new ConcurrentHashMap[String, Offset]()
    committedOffsets.asScala.foreach { item =>
      res.put(item._1, item._2)
    }
    res
  }


  @volatile private var skipTable = false
  @volatile private var currentTable: RawTableInfo = _
  @volatile private var markClose: AtomicBoolean = new AtomicBoolean(false)
  @volatile private var markPause: AtomicBoolean = new AtomicBoolean(false)

  private val connections = new ArrayBuffer[Socket]()

  protected val currentQueueSize = new AtomicLong(0)


  def isClosed = {
    markClose.get()
  }

  def setMaxBinlogQueueSize(value: Long) = {
    this.maxBinlogQueueSize = value
  }

  def setAheadLogBufferFlushSize(value: Long) = {
    this.aheadLogBufferFlushSize = value
  }

  def setAheadLogSaveTime(value: Long) = {
    this.aheadLogSaveTime = value
  }

  private val tableInfoCache = new ConcurrentHashMap[RawTableInfoCacheKey, RawTableInfo]()

  def assertTable = {
    if (currentTable == null) {
      throw new RuntimeException("No table information is available for this event, cannot process further.")
    }
  }

  def flushAheadLog = {
    synchronized {
      var buff = new ArrayBuffer[RawEvent]()
      var item = aheadLogBuffer.poll()
      while (item != null) {
        buff += item
        item = aheadLogBuffer.poll()
      }
      if (!buff.isEmpty) {
        buff.groupBy(f => f.key()).foreach { wow =>
          val key = wow._1
          val items = wow._2
          if (!writeAheadLogMap.containsKey(key)) {
            createWriteAheadLog(key)
          }
          items.foreach { event =>
            if (!committedOffsets.containsKey(event.key) || less(committedOffsets.get(event.key()), event.pos())) {
              committedOffsets.put(event.key(), event.pos())
            }
          }
          logInfo(s"writeAheadLogMap flush: key[${key}] records[${items.size}] committedOffsets[${committedOffsets.get(key)}]")
          writeAheadLogMap.get(key).write(items)
        }

      }
    }

  }

  def less(a: Offset, b: Offset): Boolean

  def addRecord(event: RawEvent): Unit = {
    //assertTable
    if (isWriteAheadStorage) {
      if (aheadLogBuffer.size >= aheadLogBufferFlushSize) {
        flushAheadLog
        writeAheadLogMap.asScala.map(f => f._2).foreach { writer =>
          writer.cleanupOldBlocks(System.currentTimeMillis() - aheadLogSaveTime)
        }
      }
    }

    if (isWriteAheadStorage) {
      if (!uncommittedOffsets.containsKey(event.key) || less(uncommittedOffsets.get(event.key()), event.pos())) {
        aheadLogBuffer.offer(event)
        uncommittedOffsets.put(event.key(), event.pos())
      }

    }

    if (!isWriteAheadStorage) {
      if (currentQueueSize.get() > maxBinlogQueueSize && !markPause.get()) {
        pause
      } else if (currentQueueSize.get() < maxBinlogQueueSize / 2 && markPause.get()) {
        resume
      }
      currentQueueSize.incrementAndGet()
      queue.offer(event)
    }
  }

  var onCommunicationFailure = (ex: Exception) => {
    logError("OnCommunicationFailure", ex)
  }

  var onConnect = () => {}

  var onDisConnect = () => {}

  var onEventDeserializationFailure = (ex: Exception) => {
    logError("onEventDeserializationFailure", ex)
  }

  def connect: Unit

  def pause: Unit

  def resume: Unit

  def closeOriginalSource: Unit

  def process(dIn: DataInputStream, dOut: DataOutputStream): Unit


  def tryWithoutException(fun: () => Unit) = {
    try {
      fun()
    } catch {
      case e: Exception =>
    }
  }

  override def close() = {
    // make sure we only close once
    if (markClose.compareAndSet(false, true)) {
      logInfo(s"Shutdown ${_server}. This may caused by the task is killed.")

      connections.foreach { socket =>
        tryWithoutException(() => {
          socket.close()
        })
      }
      connections.clear()

      closeOriginalSource

      if (_server != null) {
        tryWithoutException(() => {
          _server.close()
        })
      }

      tryWithoutException(() => {
        queue.clear()
      })

      tryWithoutException(() => {
        writeAheadLogMap.asScala.map(f => f._2).foreach { writer =>
          writer.stop()
        }
        writeAheadLogMap.clear()
      })

    }
  }


  def handleConnection(socket: Socket): Unit = {
    connections += socket
    socket.setKeepAlive(true)
    val dIn = new DataInputStream(socket.getInputStream)
    val dOut = new DataOutputStream(socket.getOutputStream)

    while (true) {
      process(dIn, dOut)
    }

  }
}

case class RawTableInfoCacheKey(databaseName: String, tableName: String, tableId: Long)
