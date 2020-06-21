package tech.mlsql.test.binlogserver

import java.io.File
import java.sql.PreparedStatement

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.mlsql.sources.mysql.binlog.MySQLConnection
import org.apache.spark.sql.mlsql.sources.mysql.binlog.MySQLConnection.Callback
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.FunSuite

/**
 * 12/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class TestMySQL extends FunSuite {

  def baseType(tableName: String, columnStr: String, f: (PreparedStatement) => Unit) = {
    FileUtils.deleteDirectory(new File("/tmp/cpl-mysql6"))
    PrepareData.createTableWithOneColumnWithDrop(tableName, columnStr)

    def insert() = {
      PrepareData.conn.executePrepare(s"insert into ${tableName} values(?)", new Callback[PreparedStatement] {
        override def execute(obj: PreparedStatement): Unit = {
          f(obj)
          obj.execute()
          Thread.sleep(5000)
        }
      }, false)
    }

    new Thread(new Runnable {
      override def run(): Unit = {
        var count = 100
        while (count > 0) {
          count -= 1
          insert
        }
      }
    }).start()
  }

  def bigdecimal(tableName: String) = {
    baseType(tableName, "name decimal", (ps) => {
      val dbc = new java.math.BigDecimal("10")
      ps.setBigDecimal(1, dbc)
    })
  }

  def bit(tableName: String) = {
    baseType(tableName, "name bit(1)", (ps) => {
      ps.setBoolean(1, true)
    })
  }


  val tableName = "binlogt"
  test("mysql") {

    bit(tableName)


    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MySQL B Sync")
      .getOrCreate()

    val df = spark.readStream.
      format("org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource").
      option("host", "127.0.0.1").
      option("port", "3306").
      option("userName", "root").
      option("password", "mlsql").
      option("databaseNamePattern", "wow").
      option("tableNamePattern", tableName).
      //      option("bingLogNamePrefix", "mysql-bin").
      //      option("binlogIndex", "16").
      //      option("binlogFileOffset", "3869").
      option("binlog.field.decode.first_name", "UTF-8").
      load()

    //    val query = df.writeStream.
    //      format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
    //      option("__path__", "/tmp/datalake/{db}/{table}").
    //      option("path", "{db}/{table}").
    //      option("mode", "Append").
    //      option("idCols", "id").
    //      option("duration", "3").
    //      option("syncType", "binlog").
    //      option("checkpointLocation", "/tmp/cpl-binlog4").
    //      outputMode("append")
    //      .trigger(Trigger.ProcessingTime("15 seconds"))
    //      .start()
    val query = df.writeStream.
      format("console").
      option("mode", "Append").
      option("truncate", "false").
      option("numRows", "100000").
      option("checkpointLocation", "/tmp/cpl-mysql6").
      outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val tableName = "binlogt"
    FileUtils.deleteDirectory(new File("/tmp/cpl-mysql6"))
    PrepareData.createTableWithOneColumnWithDrop(tableName, "name decimal")

    def insert() = {
      PrepareData.conn.executePrepare(s"insert into ${tableName} values(?)", new Callback[PreparedStatement] {
        override def execute(obj: PreparedStatement): Unit = {
          val dbc = new java.math.BigDecimal("10")
          obj.setBigDecimal(1, dbc)
          obj.execute()
          Thread.sleep(5000)
        }
      }, false)
    }

    insert
    PrepareData.conn.close()
  }
}

object PrepareData {
  val conn = new MySQLConnection("127.0.0.1", 3306, "root", "mlsql")
  conn.execute("use wow;")

  def createTableWithOneColumnWithDrop(tableName: String, column: String) = {
    conn.execute(s"DROP TABLE if EXISTS  ${tableName};")
    conn.execute(
      s"""|create table ${tableName}(
          |   ${column}
          |);
          |""".stripMargin)
  }
}
