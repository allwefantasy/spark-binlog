package org.apache.spark.sql.mlsql.sources.mysql.binlog.io

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types._

/**
 * 2019-06-14 WilliamZhu(allwefantasy@gmail.com)
 */
class SchemaTool(json: String, index: Int, _timeZone: String, hadoopConf: Configuration) {
  val schema = DataType.fromJson(json).asInstanceOf[StructType]


  def getColumnNameByIndex() = {
    schema(index).name
  }

  def timeZone = _timeZone

  def isBinary() = {
    schema(index).dataType == BinaryType
  }

  def isTimestamp() = {
    schema(index).dataType == TimestampType
  }

  def isDate() = {
    schema(index).dataType == DateType
  }

  def charset: String = {
    val allType = hadoopConf.get(s"binlog.field.decode.*")
    if (allType != null) {
      return allType
    }
    val fieldType = hadoopConf.get(s"binlog.field.decode.${getColumnNameByIndex}", "utf-8")
    return fieldType
  }

}
