package tech.mlsql.binlog.hbase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.FunSuite

/**
 * 12/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class Test extends FunSuite {
  test("hbase") {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("HBase WAL Sync")
      .getOrCreate()

    val df = spark.readStream.
      format("org.apache.spark.sql.mlsql.sources.hbase.MLSQLHBaseWALDataSource").
      option("walLogPath", "/Users/allwefantasy/Softwares/hbase-2.1.8/WALs").
      option("oldWALLogPath", "/Users/allwefantasy/Softwares/hbase-2.1.8/oldWALs").
      option("startTime", "1").
      option("databaseNamePattern", "default").
      option("tableNamePattern", "mlsql_example").
      load()

    val query = df.writeStream.
      format("console").
      option("mode", "Append").
      option("truncate", "false").
      option("numRows", "100000").
      option("checkpointLocation", "/tmp/cpl-binlog25").
      outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}
