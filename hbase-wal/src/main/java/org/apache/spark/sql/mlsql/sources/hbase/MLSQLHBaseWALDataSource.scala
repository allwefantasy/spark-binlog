package org.apache.spark.sql.mlsql.sources.hbase

import java.io.DataOutputStream
import java.net.Socket
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.mlsql.sources.hbase.wal.{HBaseWALSocketServerInExecutor, ShutDownServer, SocketClient}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.{SerializableConfiguration, TaskCompletionListener, TaskFailureListener}
import org.apache.spark.{SparkEnv, TaskContext}
import tech.mlsql.common.utils.distribute.socket.server.{ReportHostAndPort, TempSocketServerInDriver}
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


      }
    }
  }

  override def shortName(): String = "hbaseWAL"
}
