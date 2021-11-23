package org.apache.spark.sql.mlsql.sources

import java.io._
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.sql.ResultSet
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.{Locale, UUID}

import com.github.shyiko.mysql.binlog.network.ServerException
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.mlsql.sources.mysql.binlog._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{SerializableConfiguration, TaskCompletionListener, TaskFailureListener}
import org.apache.spark.{SparkEnv, TaskContext}
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.common.utils.path.PathFun

/**
 * This Datasource is used to consume MySQL binlog. Not support MariaDB yet because the connector we are using is
 * lack of the ability.
 * If you want to use this to upsert delta table, please set MySQL binlog_row_image to full so we can get the complete
 * record after updating.
 */
class MLSQLBinLogDataSource extends StreamSourceProvider with DataSourceRegister with Logging {


  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    require(schema.isEmpty, "Kafka source has a fixed schema and cannot be set with a custom one")
    (shortName(), {
      StructType(Seq(StructField("value", StringType)))
    })
  }

  /**
   * First, we will launch a task to
   *    1. start binlog client and setup a queue (where we put the binlog event)
   *    2. start a new socket the the executor where the task runs on, and return the connection message.
   * Second, Launch the MLSQLBinLogSource to consume the events:
   *    3. MLSQLBinLogSource get the host/port message and connect it to fetch the data.
   *    4. For now ,we will not support continue streaming.
   */
  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {

    val spark = sqlContext.sparkSession
    val timezoneID = spark.sessionState.conf.sessionLocalTimeZone

    val bingLogHost = parameters("host")
    val bingLogPort = parameters("port").toInt
    val bingLogUserName = parameters("userName")
    val bingLogPassword = parameters("password")


    var bingLogNamePrefix = parameters.get("bingLogNamePrefix")

    val databaseNamePattern = parameters.get("databaseNamePattern")
    val tableNamePattern = parameters.get("tableNamePattern")

    val startingOffsets = (parameters.get("startingOffsets") match {
      case Some(value) => Option(value)
      case None =>
        (parameters.get("binlogIndex"), parameters.get("binlogFileOffset")) match {
          case (Some(index), Some(pos)) => Option(BinlogOffset(index.toLong, pos.toLong).offset.toString)
          case (Some(index), None) => Option(BinlogOffset(index.toLong, 4).offset.toString)
          case _ =>
            var prefix: Option[String] = None
            var index: Option[Long] = None
            var pos: Option[Long] = None

            val tempConn = new MySQLConnection(bingLogHost, bingLogPort, bingLogUserName, bingLogPassword)
            tempConn.query("show master status;", new MySQLConnection.Callback[ResultSet] {
              override def execute(rs: ResultSet): Unit = {
                if (rs.next()) {
                  val file = rs.getString("File")
                  pos = Option(rs.getLong("Position"))
                  val Array(_prefix, _index) = file.split("\\.")
                  prefix = Option(_prefix)
                  index = Option(_index.toLong)
                }
              }
            })
            logInfo(s"Auto get prefix[${prefix}]: index:[${index}] pos:${pos}")
            if (prefix == None || index == None || pos == None) None
            bingLogNamePrefix = prefix
            Option(BinlogOffset(index.get, pos.get).offset.toString)
        }
    }).map(f => LongOffset(f.toLong))

    parameters.get("startingOffsets").map(f => LongOffset(f.toLong))

    startingOffsets match {
      case Some(value) =>
        assert(value.offset.toString.length >= 14, "The startingOffsets is combined at least 14 numbers. " +
          "The first six numbers are fileId, the left thirteen numbers are file line number.")
      case None =>
    }

    val checkPointDir = metadataPath.stripSuffix("/").split("/").
      dropRight(2).mkString("/")

    def getOffsetFromCk: Option[Long] = {
      // [bugfix] checkpointLocation:offsets is not a valid DFS filename. reason: File.pathSeparator is ':' or ';'
      // val offsetPath = PathFun(checkPointDir).add("offsets").toPath
      val offsetPath = new Path(checkPointDir.stripSuffix(Path.SEPARATOR), "offsets").toString
      logInfo(s"from checkpoint accquire offsetPath: ${offsetPath}")

      val files = HDFSOperator.listFiles(offsetPath)
      if (files.isEmpty) {
        logInfo(s"OffsetPath: ${offsetPath} checkpoint not found!")
        return None
      }
      val lastFile = files
        .filterNot(f => f.getPath.getName.endsWith(".tmp.crc") || f.getPath.getName.endsWith(".tmp"))
        .map { fileName => (fileName.getPath.getName.split("/").last.toInt, fileName.getPath) }
        .sortBy(f => f._1)
        .last._2
      val content = HDFSOperator.readFile(lastFile.toString)
      Some(content.split("\n").last.toLong)
    }

    val offsetFromCk = try {
      getOffsetFromCk match {
        case Some(checkponit) => Option(LongOffset(checkponit))
        case _ => None
      }
    } catch {
      case e: Exception =>
        logError(e.getMessage, e)
        None
    }

    val finalStartingOffsets = if (offsetFromCk.isDefined) offsetFromCk else startingOffsets

    assert(finalStartingOffsets.isDefined == bingLogNamePrefix.isDefined,
      "startingOffsets(or Checkpoint have offset files) and bingLogNamePrefix should exists together ")

    val startOffsetInFile = finalStartingOffsets.map(f => BinlogOffset.fromOffset(f.offset))
    val binlogFilename = startOffsetInFile.map(f => BinlogOffset.toFileName(bingLogNamePrefix.get, f.fileId))
    val binlogPos = startOffsetInFile.map(_.pos)

    val executorBinlogServerInfoRef = new AtomicReference[ReportBinlogSocketServerHostAndPort]()
    val tempSocketServerInDriver = new TempSocketServerInDriver(executorBinlogServerInfoRef)

    val tempSocketServerHost = tempSocketServerInDriver.host
    val tempSocketServerPort = tempSocketServerInDriver.port


    val maxBinlogQueueSize = parameters.getOrElse("maxBinlogQueueSize", "500000").toLong

    val binlogServerId = UUID.randomUUID().toString

    val hadoopConfig = spark.sparkContext.hadoopConfiguration

    parameters.filter(f => f._1.startsWith("binlog.field.decode")).
      foreach(f => hadoopConfig.set(f._1, f._2))

    val broadcastedHadoopConf = new SerializableConfiguration(hadoopConfig)

    val binaryLogClientParameters = CaseInsensitiveMap[String](parameters.filter(f => f._1.startsWith("binaryLogClient.".toLowerCase(Locale.ROOT))).
      map(f => (f._1.substring("binaryLogClient.".length), f._2)).toMap)

    def launchBinlogServer = {
      spark.sparkContext.setJobGroup(binlogServerId, s"binlog server (${bingLogHost}:${bingLogPort})", true)
      spark.sparkContext.parallelize(Seq("launch-binlog-socket-server"), 1).map { item =>

        val taskContextRef: AtomicReference[TaskContext] = new AtomicReference[TaskContext]()
        taskContextRef.set(TaskContext.get())
        val BAD_BINLOG_ERROR_CODE = 1236
        val executorBinlogServer = new BinLogSocketServerInExecutor(taskContextRef, checkPointDir, timezoneID, broadcastedHadoopConf.value)
        executorBinlogServer.setMaxBinlogQueueSize(maxBinlogQueueSize)
        executorBinlogServer.onMySQLCommunicationFailure = (ex: Exception) => {
          if (ex.isInstanceOf[ServerException]) {
            if (ex.asInstanceOf[ServerException].getErrorCode() == BAD_BINLOG_ERROR_CODE) {
              throw new RuntimeException(ex)
            }
          }
          ex.printStackTrace()
        }

        /**
         * Add callback logic code about closing binlog server when spark task goes wrong.
         */
        TaskContext.get().addTaskFailureListener(new TaskFailureListener {
          override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
            taskContextRef.set(null)
            val socket = new Socket(executorBinlogServer.host, executorBinlogServer.port)
            val out = new DataOutputStream(socket.getOutputStream)
            BinLogSocketServerCommand.sendRequest(out, ShutdownBinlogServer())
            socket.close()
          }
        })

        /**
         * Add callback logic code about closing binlog server when spark task has been done.
         */
        TaskContext.get().addTaskCompletionListener(new TaskCompletionListener {
          override def onTaskCompletion(context: TaskContext): Unit = {
            taskContextRef.set(null)
            val socket = new Socket(executorBinlogServer.host, executorBinlogServer.port)
            val out = new DataOutputStream(socket.getOutputStream)
            BinLogSocketServerCommand.sendRequest(out, ShutdownBinlogServer())
            socket.close()
          }
        })

        val socket = new Socket(tempSocketServerHost, tempSocketServerPort)
        val dout = new DataOutputStream(socket.getOutputStream)
        BinLogSocketServerCommand.sendRequest(dout,
          ReportBinlogSocketServerHostAndPort(executorBinlogServer.host, executorBinlogServer.port))
        socket.close()

        SocketServerInExecutor.addNewBinlogServer(
          MySQLBinlogServer(bingLogHost, bingLogPort),
          executorBinlogServer)

        executorBinlogServer.connectMySQL(MySQLConnectionInfo(
          bingLogHost, bingLogPort,
          bingLogUserName, bingLogPassword,
          binlogFilename, binlogPos,
          databaseNamePattern, tableNamePattern, Option(binaryLogClientParameters)), async = true)

        while (!TaskContext.get().isInterrupted() && !executorBinlogServer.isClosed) {
          Thread.sleep(1000)
        }

        ExecutorBinlogServer(executorBinlogServer.host, executorBinlogServer.port)
      }.collect()
    }

    new Thread("launch-binlog-socket-server-in-spark-job") {
      setDaemon(true)

      override def run(): Unit = {
        launchBinlogServer
      }
    }.start()

    var count = 60
    var executorBinlogServer: ExecutorBinlogServer = null
    while (executorBinlogServerInfoRef.get() == null) {
      Thread.sleep(1000)
      count -= 1
    }
    if (executorBinlogServerInfoRef.get() == null) {
      throw new RuntimeException("start BinLogSocketServerInExecutor fail")
    }
    val report = executorBinlogServerInfoRef.get()
    executorBinlogServer = ExecutorBinlogServer(report.host, report.port)
    MLSQLBinLogSource(executorBinlogServer, sqlContext.sparkSession, metadataPath, finalStartingOffsets, parameters ++ Map("binlogServerId" -> binlogServerId))
  }

  override def shortName(): String = "mysql-binlog"
}

/**
 * This implementation will not work in production. We should do more thing on
 * something like fault recovery.
 *
 * @param executorBinlogServer
 * @param spark
 * @param parameters
 */
case class MLSQLBinLogSource(executorBinlogServer: ExecutorBinlogServer,
                             spark: SparkSession,
                             metadataPath: String,
                             startingOffsets: Option[LongOffset],
                             parameters: Map[String, String]
                            ) extends Source with BinLogSocketServerSerDer with Logging {


  private val VERSION = 1
  private val initialized: AtomicBoolean = new AtomicBoolean(false)

  private var socket: Socket = null
  private var dIn: DataInputStream = null
  private var dOut: DataOutputStream = null

  private val sparkEnv = SparkEnv.get

  private var currentPartitionOffsets: Option[LongOffset] = None

  private def initialize(): Unit = synchronized {
    socket = new Socket(executorBinlogServer.host, executorBinlogServer.port)
    dIn = new DataInputStream(socket.getInputStream)
    dOut = new DataOutputStream(socket.getOutputStream)
  }

  override def schema: StructType = {
    StructType(Seq(StructField("value", StringType)))
  }

  def request(req: Request) = {
    sendRequest(dOut, req)
    readResponse(dIn)
  }

  private lazy val initialPartitionOffsets = {
    val sqlContext = spark.sqlContext
    val offsetMetadataPath = metadataPath + "/binlog-offsets"
    val metadataLog = new HDFSMetadataLog[LongOffset](sqlContext.sparkSession, offsetMetadataPath) {
      override def serialize(metadata: LongOffset, out: OutputStream): Unit = {
        out.write(0) // A zero byte is written to support Spark 2.1.0 (SPARK-19517)
        val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
        writer.write("v" + VERSION + "\n")
        writer.write(metadata.json)
        writer.flush
      }

      override def deserialize(in: InputStream): LongOffset = {
        in.read() // A zero byte is read to support Spark 2.1.0 (SPARK-19517)
        val content = IOUtils.toString(new InputStreamReader(in, StandardCharsets.UTF_8))
        // HDFSMetadataLog guarantees that it never creates a partial file.
        assert(content.length != 0)
        if (content(0) == 'v') {
          val indexOfNewLine = content.indexOf("\n")
          if (indexOfNewLine > 0) {
            val version = validateVersion(content.substring(0, indexOfNewLine), VERSION)
            LongOffset(SerializedOffset(content.substring(indexOfNewLine + 1)))
          } else {
            throw new IllegalStateException(
              s"Log file was malformed: failed to detect the log file version line.")
          }
        } else {
          // The log was generated by Spark 2.1.0
          LongOffset(SerializedOffset(content))
        }
      }
    }

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
    sendRequest(dOut, RequestOffset())
    val response = readResponse(dIn).asInstanceOf[OffsetResponse]
    LongOffset(response.currentOffset)
  }

  /**
   * Convert generic Offset to LongOffset if possible
   * Note: Since spark 3.1 started, the object class of LongOffset removed the convert method and added this method for code consistency
   * @return converted LongOffset
   */
  def convert(offset: Offset): Option[LongOffset] = offset match {
    case lo: LongOffset => Some(lo)
    case so: SerializedOffset => Some(LongOffset(so))
    case _ => None
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

    val untilPartitionOffsets = convert(end)

    // On recovery, getBatch will get called before getOffset
    if (currentPartitionOffsets.isEmpty) {
      currentPartitionOffsets = untilPartitionOffsets
    }

    if (start.isDefined && start.get == end) {
      return spark.sqlContext.internalCreateDataFrame(
        spark.sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"), schema, isStreaming = true)
    }

    // once we have changed checkpoint path, then we can start from provided starting offset.
    // In normal case, we will recover the start from checkpoint offset directory
    val fromPartitionOffsets = start match {
      case Some(prevBatchEndOffset) => convert(prevBatchEndOffset)
      case None => Some(initialPartitionOffsets)
    }

    val executorBinlogServerCopy = executorBinlogServer.copy()

    // Here we only use one partition to fetch data from binlog server.
    // The socket may be broken because we fetch all data in memory.
    // todo: optimize the way to fetch data
    val rdd = spark.sparkContext.parallelize(Seq("fetch-bing-log"), 1).mapPartitions { iter =>
      val consumer = ExecutorBinlogServerConsumerCache.acquire(executorBinlogServerCopy)
      consumer.fetchData(fromPartitionOffsets.get, untilPartitionOffsets.get)
    }.map { cr =>
      InternalRow(UTF8String.fromString(cr))
    }
    spark.sqlContext.internalCreateDataFrame(rdd.setName("mysql-bin-log"), schema, isStreaming = true)
  }

  override def stop(): Unit = {
    // when the structure streaming is stopped(caused by  exception or manually killed),
    // we should make sure the binlog server is also killed.
    // here we use the binlogServerId as the spark job group id, and so we can cancel it.
    // Also notice that we should close the socket which we use to fetch the offset.
    try {
      spark.sparkContext.cancelJobGroup(parameters("binlogServerId"))
      socket.close()
    } catch {
      case e: Exception =>
        logError("", e)
    }

  }
}


case class ExecutorInternalBinlogConsumer(executorBinlogServer: ExecutorBinlogServer) extends BinLogSocketServerSerDer {
  val socket = new Socket(executorBinlogServer.host, executorBinlogServer.port)
  val dIn = new DataInputStream(socket.getInputStream)
  val dOut = new DataOutputStream(socket.getOutputStream)
  @volatile var inUse = true
  @volatile var markedForClose = false

  def fetchData(start: LongOffset, end: LongOffset) = {
    try {
      sendRequest(dOut, RequestData(
        start.offset,
        end.offset))
      // todo: optimize the way to fetch data
      //      val response = readResponse(dIn)
      val response = readIteratedResponse(dIn)
      //      response.asInstanceOf[DataResponse].data
      response.flatMap(f => f.asInstanceOf[DataResponse].data)
    } finally {
      ExecutorBinlogServerConsumerCache.release(this)
    }
  }

  def close = {
    socket.close()
  }
}










