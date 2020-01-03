package tech.mlsql.test.binlogserver

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.FunSuite

/**
 * 12/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class TestMySQL extends FunSuite {
  test("mysql") {
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
      option("tableNamePattern", "users").
      option("bingLogNamePrefix", "mysql-bin").
      option("binlogIndex", "1").
      option("binlogFileOffset", "882").
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

object Main{
  def main(args: Array[String]): Unit = {
    println(new Timestamp(157696374))
  }
}
