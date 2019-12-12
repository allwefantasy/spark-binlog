package tech.mlsql.binlog.hbase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
 * 12/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Binlog2DeltaTest")
      .getOrCreate()

    val df = spark.readStream.
      format("org.apache.spark.sql.mlsql.sources.hbase.MLSQLHBaseWALDataSource").
      option("host", "127.0.0.1").
      option("port", "3306").
      option("userName", "root").
      option("password", "123456").
      option("databaseNamePattern", "test").
      option("tableNamePattern", "mlsql_binlog").
      option("bingLogNamePrefix", "mysql-bin").
      option("binlogIndex", "10").
      option("binlogFileOffset", "90840").
      load()

    val query = df.writeStream.
      format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
      option("path", "/tmp/jack").
      option("mode", "Append").
      option("duration", "3").
      option("checkpointLocation", "/tmp/cpl-binlog2").
      outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}
