package org.apache.spark.sql.mlsql.sources.mysql.binlog

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.regex.Pattern

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.EventType._
import com.github.shyiko.mysql.binlog.event._
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.mlsql.sources.mysql.binlog.io.{DeleteRowsWriter, EventInfo, InsertRowsWriter, UpdateRowsWriter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.BinlogWriteAheadLog

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * 2019-06-13 WilliamZhu(allwefantasy@gmail.com)
 */
class BinLogSocketServerInExecutor[T](taskContextRef: AtomicReference[T],
                                      checkpointDir: String,
                                      timezone: String,
                                      hadoopConf: Configuration,
                                      isWriteAheadStorage: Boolean = true)
  extends SocketServerInExecutor[T](taskContextRef, "binlog-socket-server-in-executor")
    with BinLogSocketServerSerDer with Logging {

  private var connectThread: Thread = null

  private var binaryLogClient: BinaryLogClient = null

  private var currentBinlogFile: String = null

  private var currentBinlogPosition: Long = 4
  private var currentBinlogPositionConsumeFlag: Boolean = false
  private var nextBinlogPosition: Long = 4

  private val queue = new util.ArrayDeque[RawBinlogEvent]()


  private val writeAheadLog = {
    val sparkEnv = SparkEnv.get
    val tmp = new BinlogWriteAheadLog(UUID.randomUUID().toString, sparkEnv.serializerManager, sparkEnv.conf, hadoopConf, checkpointDir)
    tmp.cleanupOldBlocks(System.currentTimeMillis(), true)
    tmp
  }

  private var databaseNamePattern: Option[Pattern] = None
  private var tableNamePattern: Option[Pattern] = None

  private var maxBinlogQueueSize: Long = 0l

  private var connect: MySQLConnectionInfo = null

  private val aheadLogBuffer = new java.util.concurrent.ConcurrentLinkedDeque[RawBinlogEvent]()


  @volatile private var skipTable = false
  @volatile private var currentTable: TableInfo = _
  @volatile private var markClose: AtomicBoolean = new AtomicBoolean(false)
  @volatile private var markPause: AtomicBoolean = new AtomicBoolean(false)

  private val connections = new ArrayBuffer[Socket]()

  private val currentQueueSize = new AtomicLong(0)

  val updateRowsWriter = new UpdateRowsWriter(timezone, hadoopConf)
  val deleteRowsWriter = new DeleteRowsWriter(timezone, hadoopConf)
  val insertRowsWriter = new InsertRowsWriter(timezone, hadoopConf)

  def isClosed = {
    markClose.get()
  }

  def setMaxBinlogQueueSize(value: Long) = {
    this.maxBinlogQueueSize = value
  }

  private val tableInfoCache = new ConcurrentHashMap[TableInfoCacheKey, TableInfo]()

  def assertTable = {
    if (currentTable == null) {
      throw new RuntimeException("No table information is available for this event, cannot process further.")
    }
  }

  def flushAheadLog = {
    synchronized {
      var buff = new ArrayBuffer[RawBinlogEvent]()
      var item = aheadLogBuffer.poll()
      while (item != null) {
        buff += item
        item = aheadLogBuffer.poll()
      }
      if (!buff.isEmpty) {
        writeAheadLog.write(buff)
      }
    }

  }

  def addRecord(event: Event, binLogFilename: String, eventType: String) = {
    assertTable
    val item = new RawBinlogEvent(event, currentTable, binLogFilename, eventType, currentBinlogPosition)
    if (isWriteAheadStorage) {
      if (aheadLogBuffer.size >= 1000) {
        flushAheadLog
        writeAheadLog.cleanupOldBlocks(System.currentTimeMillis() - 1000 * 60 * 60)
      }
    }

    if (isWriteAheadStorage) {
      aheadLogBuffer.offer(item)
    }

    if (!isWriteAheadStorage) {
      if (currentQueueSize.get() > maxBinlogQueueSize && !markPause.get()) {
        pause
      } else if (currentQueueSize.get() < maxBinlogQueueSize / 2 && markPause.get()) {
        resume
      }
      currentQueueSize.incrementAndGet()
      queue.offer(item)
    }
  }

  var onMySQLCommunicationFailure = (ex: Exception) => {
    logError("OnMySQLCommunicationFailure", ex)
  }

  var onMySQLConnect = () => {}

  var onMySQLDisConnect = () => {}

  var onMySQLEventDeserializationFailure = (ex: Exception) => {
    logError("onMySQLEventDeserializationFailure", ex)
  }

  private def _connectMySQL(connect: MySQLConnectionInfo) = {
    binaryLogClient = new BinaryLogClient(connect.host, connect.port, connect.userName, connect.password)
    connect.binlogFileName match {
      case Some(filename) =>
        binaryLogClient.setBinlogFilename(filename)
        currentBinlogFile = filename
      case _ =>
    }

    val blcProperties = connect.properties.get
    if (blcProperties.contains("heartbeatInterval")) {
      binaryLogClient.setHeartbeatInterval(blcProperties("heartbeatInterval").toLong)
    }
    if (blcProperties.contains("blocking")) {
      binaryLogClient.setBlocking(blcProperties("blocking").toBoolean)
    }

    if (blcProperties.contains("connectTimeout")) {
      binaryLogClient.setConnectTimeout(blcProperties("connectTimeout").toLong)
    }

    if (blcProperties.contains("keepAlive")) {
      binaryLogClient.setKeepAlive(blcProperties("keepAlive").toBoolean)
    }


    connect.recordPos match {
      case Some(recordPos) => binaryLogClient.setBinlogPosition(recordPos)
      case _ =>
    }

    binaryLogClient.getHeartbeatInterval

    val eventDeserializer = new EventDeserializer()
    eventDeserializer.setCompatibilityMode(
      //EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO,
      EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
      //EventDeserializer.CompatibilityMode.INVALID_DATE_AND_TIME_AS_MIN_VALUE
    )
    binaryLogClient.setEventDeserializer(eventDeserializer)

    binaryLogClient.registerLifecycleListener(new BinaryLogClient.LifecycleListener() {
      override def onCommunicationFailure(client: BinaryLogClient, ex: Exception): Unit = {
        onMySQLCommunicationFailure(ex)
      }

      override def onConnect(client: BinaryLogClient): Unit = {
        onMySQLConnect()
      }

      override def onEventDeserializationFailure(client: BinaryLogClient, ex: Exception): Unit = {
        onMySQLEventDeserializationFailure(ex)
      }

      override def onDisconnect(client: BinaryLogClient): Unit = {
        onMySQLDisConnect()
      }
    })

    //for now, we only care insert/update/delete three kinds of event
    binaryLogClient.registerEventListener(new BinaryLogClient.EventListener() {
      def onEvent(event: Event): Unit = {
        val header = event.getHeader[EventHeaderV4]()
        val eventType = header.getEventType
        if (eventType != ROTATE && eventType != FORMAT_DESCRIPTION) {
          currentBinlogPosition = header.getPosition
          currentBinlogPositionConsumeFlag = true
          nextBinlogPosition = header.getNextPosition
        }

        eventType match {
          case TABLE_MAP =>
            val data = event.getData[TableMapEventData]()
            skipTable = !(databaseNamePattern
              .map(_.matcher(data.getDatabase).matches())
              .getOrElse(false) && tableNamePattern
              .map(_.matcher(data.getTable).matches())
              .getOrElse(false))

            if (!skipTable) {
              val cacheKey = new TableInfoCacheKey(data.getDatabase, data.getTable, data.getTableId)
              currentTable = tableInfoCache.get(cacheKey)
              if (currentTable == null) {
                val tableSchemaInfo = loadSchemaInfo(connect, cacheKey)
                val currentTableRef = new TableInfo(cacheKey.databaseName, cacheKey.tableName, cacheKey.tableId, tableSchemaInfo.json)
                tableInfoCache.put(cacheKey, currentTableRef)
                currentTable = currentTableRef
              }

            } else currentTable = null

          case _type if (isWrite(_type)) =>
            if (!skipTable) addRecord(event, binaryLogClient.getBinlogFilename, EventInfo.INSERT_EVENT)

          case _type if (isUpdate(_type)) =>
            if (!skipTable) {
              addRecord(event, binaryLogClient.getBinlogFilename, EventInfo.UPDATE_EVENT)
            }

          case _type if (isDelete(_type)) =>
            if (!skipTable) {
              addRecord(event, binaryLogClient.getBinlogFilename, EventInfo.DELETE_EVENT)
            }

          case ROTATE =>
            val rotateEventData = event.getData[RotateEventData]()
            currentBinlogFile = rotateEventData.getBinlogFilename
            currentBinlogPosition = rotateEventData.getBinlogPosition
          case _ =>
        }
      }
    })

    binaryLogClient.connect()
  }

  def loadSchemaInfo(connectionInfo: MySQLConnectionInfo, table: TableInfoCacheKey): StructType = {
    val parameters = Map(
      "url" -> s"jdbc:mysql://${connectionInfo.host}:${connectionInfo.port}?useUnicode=true&zeroDateTimeBehavior=convertToNull&characterEncoding=UTF-8&tinyInt1isBit=false",
      "user" -> connectionInfo.userName,
      "password" -> connectionInfo.password,
      "dbtable" -> s"${table.databaseName}.${table.tableName}",
      "driver" -> "com.mysql.jdbc.Driver"
    )
    val jdbcOptions = new JDBCOptions(parameters)
    val schema = JDBCRDD.resolveTable(jdbcOptions)
    schema
  }

  def connectMySQL(_connect: MySQLConnectionInfo, async: Boolean = true) = {
    connect = _connect
    databaseNamePattern = connect.databaseNamePattern.map(Pattern.compile)

    tableNamePattern = connect.tableNamePattern.map(Pattern.compile)
    if (async) {
      connectThread = new Thread(s"connect mysql(${connect.host}, ${connect.port}) ") {
        setDaemon(true)

        override def run(): Unit = {
          try {
            // todo: should be able to reattempt, because when MySQL is busy, it's hard to connect
            _connectMySQL(connect)
          } catch {
            case e: Exception =>
              throw e
          }

        }
      }
      connectThread.start()
    } else {
      _connectMySQL(connect)
    }

  }

  private def toOffset(rawBinlogEvent: RawBinlogEvent) = {
    BinlogOffset.fromFileAndPos(rawBinlogEvent.getBinlogFilename, rawBinlogEvent.getPos).offset
  }

  def convertRawBinlogEventRecord(rawBinlogEvent: RawBinlogEvent) = {
    val writer = rawBinlogEvent.getEventType() match {
      case "insert" => insertRowsWriter
      case "update" => updateRowsWriter
      case "delete" => deleteRowsWriter
    }

    val jsonList = writer.writeEvent(rawBinlogEvent)
    //    try {
    //
    //    } catch {
    //      case e: Exception =>
    //        logError("", e)
    //        new util.ArrayList[String]()
    //    }
    jsonList
  }

  def tryWithoutException(fun: () => Unit) = {
    try {
      fun()
    } catch {
      case e: Exception =>
    }
  }

  def pause = {
    if (markPause.compareAndSet(false, true)) {
      if (binaryLogClient != null) {
        tryWithoutException(() => {
          binaryLogClient.disconnect()
        })
      }
    }
  }

  def resume = {
    if (markPause.compareAndSet(true, false)) {
      connectThread = new Thread(s"connect mysql(${connect.host}, ${connect.port}) ") {
        setDaemon(true)

        override def run(): Unit = {
          try {
            // todo: should be able to reattempt, because when MySQL is busy, it's hard to connect
            binaryLogClient.connect()
          } catch {
            case e: Exception =>
              throw e
          }

        }
      }
      connectThread.start()
    }
  }

  override def close() = {
    // make sure we only close once
    if (markClose.compareAndSet(false, true)) {
      logInfo(s"Shutdown ${server}. This may caused by the task is killed.")

      connections.foreach { socket =>
        tryWithoutException(() => {
          socket.close()
        })
      }
      connections.clear()

      if (binaryLogClient != null) {
        tryWithoutException(() => {
          binaryLogClient.disconnect()
        })
      }

      if (server != null) {
        tryWithoutException(() => {
          server.close()
        })
      }

      tryWithoutException(() => {
        queue.clear()
      })

      tryWithoutException(() => {
        writeAheadLog.stop()
      })


    }
  }

  def handleConnection(socket: Socket): Unit = {
    connections += socket
    socket.setKeepAlive(true)
    val dIn = new DataInputStream(socket.getInputStream)
    val dOut = new DataOutputStream(socket.getOutputStream)

    while (true) {
      readRequest(dIn) match {
        case _: ShutdownBinlogServer =>
          close()
        case _: RequestQueueSize => {
          sendResponse(dOut, QueueSizeResponse(queue.size()))
        }
        case _: RequestOffset =>
          var count = 5
          while (!currentBinlogPositionConsumeFlag && count > 0) {
            Thread.sleep(1000)
            count -= 1
          }

          val currentNextBinlogPosition = if (nextBinlogPosition == 4) currentBinlogPosition else nextBinlogPosition;

          if (count <= 0) {
            logInfo(s"Can not wait message in ${currentNextBinlogPosition}")
          }
          flushAheadLog
          sendResponse(dOut, OffsetResponse(BinlogOffset.fromFileAndPos(currentBinlogFile, currentNextBinlogPosition).offset))


        case request: RequestData =>
          val start = request.startOffset
          val end = request.endOffset

          //          val res = ArrayBuffer[String]()
          try {

            if (isWriteAheadStorage) {
              sendMark(dOut, SocketReplyMark.HEAD) // 先发一个开始位
              writeAheadLog.read((records) => {
                records.foreach { _record =>
                  val record = _record.asInstanceOf[RawBinlogEvent]
                  if (toOffset(record) >= start && toOffset(record) < end) {
                    //                    res ++= convertRawBinlogEventRecord(record).asScala
                    iterativeSendData(dOut, DataResponse(convertRawBinlogEventRecord(record).asScala.toList))
                  }
                }
              })
              sendMark(dOut, SocketReplyMark.END) // 再发一个结束位
            } else {
              sendMark(dOut, SocketReplyMark.HEAD)
              var item: RawBinlogEvent = queue.poll()
              while (item != null && toOffset(item) >= start && toOffset(item) < end) {
                iterativeSendData(dOut, DataResponse(convertRawBinlogEventRecord(item).asScala.toList))
                item = queue.poll()
                currentQueueSize.decrementAndGet()
              }
              sendMark(dOut, SocketReplyMark.END)
            }

          } catch {
            case e: Exception =>
              logError("", e)
              close
          }
        //          sendResponse(dOut, DataResponse(res.toList))
      }
    }

  }
}

object BinLogSocketServerInExecutor {
  val FILE_NAME_NOT_SET = "file_name_not_set"
}
