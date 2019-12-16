package tech.mlsql.binlog.common

import tech.mlsql.common.utils.distribute.socket.server.ReportHostAndPort

/**
 * 12/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
trait BinlogConsumer {

  def inUse: Boolean

  def markInUse: Unit

  def markInIdle: Unit

  def markedForClose: Unit

  def isClose: Boolean

  def fetchData(partitionId: String, start: Long, end: Long): Any

  def close: Unit

  def targetHostAndPort: ReportHostAndPort
}
