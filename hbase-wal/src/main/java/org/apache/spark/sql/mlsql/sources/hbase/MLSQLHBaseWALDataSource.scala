package org.apache.spark.sql.mlsql.sources.hbase

import java.io._
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.regex.Pattern

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.mlsql.sources.hbase.wal._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, LaunchSourceConsumerAndProducer, SQLContext, SparkSession}
import org.apache.spark.unsafe.types.UTF8String
import tech.mlsql.binlog.common._
import tech.mlsql.common.utils.distribute.socket.server.ReportHostAndPort

/**
 * 9/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLHBaseWALDataSource extends StreamSourceProvider with DataSourceRegister {


  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    require(schema.isEmpty, "HBase WAL source has a fixed schema and cannot be set with a custom one")
    (shortName(), StructType(Seq(StructField("value", StringType))))
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {


    val spark = sqlContext.sparkSession

    val walLogPath = parameters("walLogPath")
    val oldWALLogPath = parameters("oldWALLogPath")
    val startTime = parameters.getOrElse("startTime", "0").toLong
    val aheadLogBufferFlushSize = parameters.getOrElse("aheadLogBufferFlushSize", "-1").toLong
    val aheadLogSaveTime = parameters.getOrElse("aheadLogSaveTime", "-1").toLong

    val databaseNamePattern = parameters.get("databaseNamePattern")
    val tableNamePattern = parameters.get("tableNamePattern")

    val launchSourceConsumerAndProducer = new LaunchSourceConsumerAndProducer(spark)
    val binlogServerId = UUID.randomUUID().toString

    val hostAndPort = launchSourceConsumerAndProducer.launch[TaskContext](metadataPath, binlogServerId,
      // create server logic
      (taskContextRef: AtomicReference[TaskContext], checkPointDir: String, conf: Configuration) => {
        val walServer = new HBaseWALSocketServerInExecutor[TaskContext](taskContextRef, checkPointDir, conf, true)
        walServer.setWalLogPath(walLogPath)
        walServer.setOldWALLogPath(oldWALLogPath)
        walServer.setStartTime(startTime)
        walServer.setDatabaseNamePattern(databaseNamePattern.map(Pattern.compile(_)))
        walServer.setTableNamePattern(tableNamePattern.map(Pattern.compile(_)))

        if (aheadLogBufferFlushSize != -1) {
          walServer.setAheadLogBufferFlushSize(aheadLogBufferFlushSize)
        }

        if (aheadLogSaveTime != -1) {
          walServer.setAheadLogBufferFlushSize(aheadLogSaveTime)
        }
        walServer
      },
      // register callback when task is interrupted then we should kill the server
      (walServer: OriginalSourceServerInExecutor[TaskContext]) => {
        val client = new SocketClient()
        val clientSocket = new Socket(walServer._host, walServer._port)
        val dout2 = new DataOutputStream(clientSocket.getOutputStream)
        client.sendRequest(dout2, ShutDownServer())
        dout2.close()
        clientSocket.close()
      })


    MLSQLHBaseWAlSource(hostAndPort, sqlContext.sparkSession, metadataPath, parameters ++ Map("binlogServerId" -> binlogServerId))
  }

  override def shortName(): String = "hbaseWAL"
}

case class MLSQLHBaseWAlSource(hostAndPort: ReportHostAndPort, spark: SparkSession,
                               metadataPath: String,
                               parameters: Map[String, String]
                              ) extends Source with Logging {


  private val VERSION = 1
  private val initialized: AtomicBoolean = new AtomicBoolean(false)

  private var socket: Socket = null
  private var dIn: DataInputStream = null
  private var dOut: DataOutputStream = null

  private var currentPartitionOffsets: Option[CommonSourceOffset] = None

  private def initialize(): Unit = synchronized {
    socket = new Socket(hostAndPort.host, hostAndPort.port)
    dIn = new DataInputStream(socket.getInputStream)
    dOut = new DataOutputStream(socket.getOutputStream)
  }

  override def schema: StructType = {
    StructType(Seq(StructField("value", StringType)))
  }

  private lazy val initialPartitionOffsets = {
    val sqlContext = spark.sqlContext
    val offsetMetadataPath = metadataPath + "/binlog-offsets"
    val metadataLog = new HDFSMetadataLog[CommonSourceOffset](sqlContext.sparkSession, offsetMetadataPath) {
      override def serialize(metadata: CommonSourceOffset, out: OutputStream): Unit = {
        out.write(0) // A zero byte is written to support Spark 2.1.0 (SPARK-19517)
        val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
        writer.write("v" + VERSION + "\n")
        writer.write(metadata.json)
        writer.flush
      }

      override def deserialize(in: InputStream): CommonSourceOffset = {
        in.read() // A zero byte is read to support Spark 2.1.0 (SPARK-19517)
        val content = IOUtils.toString(new InputStreamReader(in, StandardCharsets.UTF_8))
        // HDFSMetadataLog guarantees that it never creates a partial file.
        assert(content.length != 0)
        if (content(0) == 'v') {
          val indexOfNewLine = content.indexOf("\n")
          if (indexOfNewLine > 0) {
            val version = parseVersion(content.substring(0, indexOfNewLine), VERSION)
            CommonSourceOffset(SerializedOffset(content.substring(indexOfNewLine + 1)))
          } else {
            throw new IllegalStateException(
              s"Log file was malformed: failed to detect the log file version line.")
          }
        } else {
          // The log was generated by Spark 2.1.0
          CommonSourceOffset(SerializedOffset(content))
        }
      }
    }


    val startingOffsets = parameters.get("startingOffsets").map(f => CommonSourceOffset(CommonSourceOffset.partitionOffsets(f)))

    metadataLog.get(0).getOrElse {
      val offsets = startingOffsets match {
        case Some(offset) => offset
        case None => getLatestOffset
      }
      metadataLog.add(0, offsets)
      logInfo(s"Initial offsets: $offsets")
      offsets
    }

  }

  def getLatestOffset = {
    val client = new SocketClient()
    client.sendRequest(dOut, RequestOffset())
    val response = client.readResponse(dIn).asInstanceOf[OffsetResponse]
    val offsets = response.offsets.map { f =>
      (new CommonPartition(f._1, f._1.hashCode), f._2.toLong)
    }.toMap
    CommonSourceOffset(offsets)
  }


  override def getOffset: Option[Offset] = {
    synchronized {
      if (initialized.compareAndSet(false, true)) {
        initialize()
      }
    }
    val latest = getLatestOffset
    currentPartitionOffsets = Some(latest)
    Some(latest)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    synchronized {
      if (initialized.compareAndSet(false, true)) {
        initialize()
      }
    }

    initialPartitionOffsets

    val untilPartitionOffsets = CommonSourceOffset(end)


    if (currentPartitionOffsets.isEmpty) {
      currentPartitionOffsets = Option(untilPartitionOffsets)
    }

    if (start.isDefined && start.get == end) {
      return spark.sqlContext.internalCreateDataFrame(
        spark.sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"), schema, isStreaming = true)
    }

    // once we have changed checkpoint path, then we can start from provided starting offset.
    // In normal case, we will recover the start from checkpoint offset directory
    val fromPartitionOffsets = start match {
      case Some(prevBatchEndOffset) =>
        CommonSourceOffset(prevBatchEndOffset)
      case None =>
        initialPartitionOffsets
    }

    val walServerHost = hostAndPort.host
    val walServerPort = hostAndPort.port


    val offsetRanges = untilPartitionOffsets.partitionToOffsets.map(f => f._1).map { tp =>
      val fromOffset = fromPartitionOffsets.partitionToOffsets.get(tp).getOrElse {
        logWarning(s"New region is found ${tp.topic()}, fromOffset from -1L")
        -1L
      }
      val untilOffset = untilPartitionOffsets.partitionToOffsets.getOrElse(tp, {
        logWarning(s"fromPartitionOffsets:${fromPartitionOffsets} untilPartitionOffsets:${untilPartitionOffsets}  " +
          s"untilPartitionOffsets do not contains ${tp}. This is normally caused by the WAL directory is empty or some region is missing")
        fromOffset
      })

      CommonOffsetRange(tp, fromOffset, untilOffset)
    }.filter { range =>
      if (range.untilOffset < range.fromOffset) {
        throw new RuntimeException(s"Partition ${range.commonPartition}'s offset was changed from " +
          s"${range.fromOffset} to ${range.untilOffset}, some data may have been missed")
        false
      }
       else {
      true
    }
    }.toArray

  val rdd = if (offsetRanges.length == 0) {
    logWarning("No offsets found in HBase WAL.")
    spark.sparkContext.emptyRDD[InternalRow]

  } else {
    spark.sparkContext.parallelize(offsetRanges.toSeq, offsetRanges.length).flatMap { range =>
      val consumer = ConsumerCache.acquire(ReportHostAndPort(walServerHost, walServerPort), () => {
        new ExecutorInternalBinlogConsumer(ReportHostAndPort(walServerHost, walServerPort))
      }
      )
      consumer
      .fetchData(range.commonPartition.topic(), range.fromOffset, range.untilOffset).asInstanceOf[Iterator[String]]
    }.map { cr =>
      InternalRow(UTF8String.fromString(cr))
    }
  }
  rdd.setName("incremental-data")
  spark.sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)

}

override def stop (): Unit = {
// when the structure streaming is stopped(caused by  exception or manually killed),
// we should make sure the binlog server is also killed.
// here we use the binlogServerId as the spark job group id, and so we can cancel it.
// Also notice that we should close the socket which we use to fetch the offset.
try {
spark.sparkContext.cancelJobGroup (parameters ("binlogServerId") )
socket.close ()
} catch {
case e: Exception =>
logError ("", e)
}

}
}

case class ExecutorInternalBinlogConsumer(hostAndPort: ReportHostAndPort) extends BinlogConsumer {

  val socket = new Socket(hostAndPort.host, hostAndPort.port)
  val dIn = new DataInputStream(socket.getInputStream)
  val dOut = new DataOutputStream(socket.getOutputStream)
  val client = new SocketClient()

  @volatile var _inUse = true
  @volatile var _markedForClose = false


  override def markInUse: Unit = _inUse = true

  override def markInIdle: Unit = _inUse = false

  override def markedForClose: Unit = _markedForClose = true

  override def isClose: Boolean = _markedForClose

  override def fetchData(partitionId: String, start: Long, end: Long): Any = {
    try {
      client.sendRequest(dOut, RequestData(partitionId,
        start,
        end))
      val response = client.readIterator(dIn)
      response.flatMap(f => f.asInstanceOf[DataResponse].data)
    } finally {
      ConsumerCache.release(this)
    }
  }

  override def close = {
    socket.close()
  }

  override def targetHostAndPort: ReportHostAndPort = hostAndPort

  override def inUse: Boolean = _inUse
}

