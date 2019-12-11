package org.apache.spark.sql.mlsql.sources.hbase

import java.io.DataOutputStream
import java.net.Socket
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.mlsql.sources.hbase.wal.{HBaseWALSocketServerInExecutor, ShutDownServer, SocketClient}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.util.{SerializableConfiguration, TaskCompletionListener, TaskFailureListener}
import org.apache.spark.{SparkEnv, TaskContext}
import tech.mlsql.common.utils.distribute.socket.server.{ReportHostAndPort, ReportSingleAction, SocketServerSerDer, TempSocketServerInDriver}
import tech.mlsql.common.utils.network.NetUtils

/**
 * 9/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLHBaseWALDataSource extends StreamSourceProvider with DataSourceRegister {


  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    require(schema.isEmpty, "Kafka source has a fixed schema and cannot be set with a custom one")
    (shortName(), {
      StructType(Seq(StructField("rawkey", StringType)))
      StructType(Seq(StructField("value", StringType)))
    })
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {
    val spark = sqlContext.sparkSession
    val conf = spark.sessionState.newHadoopConf()
    val confBr = spark.sparkContext.broadcast(new SerializableConfiguration(conf))

    val binlogServerId = UUID.randomUUID().toString
    val timezoneID = spark.sessionState.conf.sessionLocalTimeZone
    val checkPointDir = metadataPath.stripSuffix("/").split("/").
      dropRight(2).mkString("/")

    val hostAndPortContext = new AtomicReference[ReportHostAndPort]()

    val tempServer = new TempSocketServerInDriver(hostAndPortContext) {
      def close = {
        _server.close()
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
    }

    val tempSocketServerHost = tempServer._host
    val tempSocketServerPort = tempServer._port


    def launchHBaseWALServer = {
      spark.sparkContext.setJobGroup(binlogServerId, s"hbase WAL server", true)
      spark.sparkContext.parallelize(Seq("launch-hbase-wal-socket-server"), 1).map { item =>
        val taskContextRef: AtomicReference[TaskContext] = new AtomicReference[TaskContext]()
        taskContextRef.set(TaskContext.get())

        val walServer = new HBaseWALSocketServerInExecutor(taskContextRef, checkPointDir, confBr.value.value, true)

        def sendStopServerRequest = {
          val client = new SocketClient()
          val clientSocket = new Socket(walServer._host, walServer._port)
          val dout2 = new DataOutputStream(clientSocket.getOutputStream)
          client.sendRequest(dout2, ShutDownServer())
          dout2.close()
          clientSocket.close()
        }

        TaskContext.get().addTaskFailureListener(new TaskFailureListener {
          override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
            taskContextRef.set(null)
            sendStopServerRequest

          }
        })

        TaskContext.get().addTaskCompletionListener(new TaskCompletionListener {
          override def onTaskCompletion(context: TaskContext): Unit = {
            taskContextRef.set(null)
            sendStopServerRequest
          }
        })

        def sendHBaseWALServerInfoBackToDriver = {
          val client = new SocketServerSerDer[ReportSingleAction, ReportSingleAction]() {}
          val socket = new Socket(tempSocketServerHost, tempSocketServerPort)
          val dout = new DataOutputStream(socket.getOutputStream)
          client.sendRequest(dout, ReportHostAndPort(walServer._host, walServer._port))
          dout.close()
          socket.close()
        }

        sendHBaseWALServerInfoBackToDriver
        while (!TaskContext.get().isInterrupted() && !walServer.isClosed) {
          Thread.sleep(1000)
        }
        ()
      }
    }

    new Thread("launch-hbase-wal-socket-server-in-spark-job") {
      setDaemon(true)

      override def run(): Unit = {
        launchHBaseWALServer
      }
    }.start()

    var count = 60

    while (hostAndPortContext.get() == null) {
      Thread.sleep(1000)
      count -= 1
    }
    if (hostAndPortContext.get() == null) {
      throw new RuntimeException("start HBaseWALSocketServerInExecutor fail")
    }
    tempServer.close
    MLSQLHBaseWAlSource(sqlContext.sparkSession, metadataPath, parameters ++ Map("binlogServerId" -> binlogServerId))
  }

  override def shortName(): String = "hbaseWAL"
}

case class MLSQLHBaseWAlSource(spark: SparkSession,
                               metadataPath: String,
                               parameters: Map[String, String]
                              ) extends Source with Logging {
  override def schema: StructType = ???

  override def getOffset: Option[Offset] = ???

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = ???

  override def stop(): Unit = ???
}
