package org.apache.spark.sql

import java.io.DataOutputStream
import java.net.Socket
import java.util.concurrent.atomic.AtomicReference

import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.{SerializableConfiguration, TaskCompletionListener, TaskFailureListener}
import org.apache.spark.{SparkEnv, TaskContext}
import tech.mlsql.binlog.common.OriginalSourceServerInExecutor
import tech.mlsql.common.utils.distribute.socket.server.{ReportHostAndPort, ReportSingleAction, SocketServerSerDer, TempSocketServerInDriver}
import tech.mlsql.common.utils.network.NetUtils


class LaunchSourceConsumerAndProducer(spark: SparkSession) {

  def launch[T](metadataPath: String,
                binlogServerId: String,
                createServer: (AtomicReference[TaskContext], String, Configuration) => OriginalSourceServerInExecutor[T],
                sendStopServerRequest: OriginalSourceServerInExecutor[T] => Unit
               ) = {
    val conf = spark.sessionState.newHadoopConf()
    val confBr = spark.sparkContext.broadcast(new SerializableConfiguration(conf))

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
      spark.sparkContext.setJobGroup(binlogServerId, s"binlog server", true)
      spark.sparkContext.parallelize(Seq("launch-binlog-socket-server"), 1).map { item =>
        val taskContextRef: AtomicReference[TaskContext] = new AtomicReference[TaskContext]()
        taskContextRef.set(TaskContext.get())

        val walServer = createServer(taskContextRef, checkPointDir, confBr.value.value)

        TaskContext.get().addTaskFailureListener(new TaskFailureListener {
          override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
            taskContextRef.set(null)
            sendStopServerRequest(walServer)

          }
        })

        TaskContext.get().addTaskCompletionListener(new TaskCompletionListener {
          override def onTaskCompletion(context: TaskContext): Unit = {
            taskContextRef.set(null)
            sendStopServerRequest(walServer)
          }
        })

        def sendBinlogServerInfoBackToDriver = {
          val client = new SocketServerSerDer[ReportSingleAction, ReportSingleAction]() {}
          val socket = new Socket(tempSocketServerHost, tempSocketServerPort)
          val dout = new DataOutputStream(socket.getOutputStream)
          client.sendRequest(dout, ReportHostAndPort(walServer._host, walServer._port))
          dout.close()
          socket.close()
        }

        sendBinlogServerInfoBackToDriver
        walServer.connect
        while (!TaskContext.get().isInterrupted() && !walServer.isClosed) {
          Thread.sleep(1000)
        }
        ""
      }.collect()
    }

    new Thread("launch-binlog-socket-server-in-spark-job") {
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
      throw new RuntimeException("Start Binlog Server Fail")
    }
    tempServer.close
    hostAndPortContext.get()
  }
}
