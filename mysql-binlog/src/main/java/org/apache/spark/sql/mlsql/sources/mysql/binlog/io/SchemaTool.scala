package org.apache.spark.sql.mlsql.sources.mysql.binlog.io

import org.apache.spark.sql.types._

/**
 * 2019-06-14 WilliamZhu(allwefantasy@gmail.com)
 */
class SchemaTool(json: String, index: Int, _timeZone: String) {
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

}
