package tech.mlsql.binlog.common

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.sql.sources.v2.reader.streaming.{Offset => OffsetV2}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

/**
 * 11/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
case class CommonSourceOffset(partitionToOffsets: Map[CommonPartition, Long]) extends OffsetV2 {

  override val json = CommonSourceOffset.partitionOffsets(partitionToOffsets)


}

object CommonSourceOffset {
  private implicit val formats = Serialization.formats(NoTypeHints)

  def partitionOffsets(partitionOffsets: Map[CommonPartition, Long]): String = {

    val result = new HashMap[String, HashMap[Int, Long]]()
    implicit val ordering = new Ordering[CommonPartition] {
      override def compare(x: CommonPartition, y: CommonPartition): Int = {
        Ordering.Tuple2[String, Int].compare((x.topic, x.partition), (y.topic, y.partition))
      }
    }
    val partitions = partitionOffsets.keySet.toSeq.sorted // sort for more determinism
    partitions.foreach { tp =>
      val off = partitionOffsets(tp)
      val parts = result.getOrElse(tp.topic, new HashMap[Int, Long])
      parts += tp.partition -> off
      result += tp.topic -> parts
    }
    Serialization.write(result)
  }

  def partitionOffsets(str: String): Map[CommonPartition, Long] = {
    try {
      Serialization.read[Map[String, Map[Int, Long]]](str).flatMap { case (topic, partOffsets) =>
        partOffsets.map { case (part, offset) =>
          new CommonPartition(topic, part) -> offset
        }
      }.toMap
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}, got $str""")
    }
  }

  def getPartitionOffsets(offset: Offset): Map[CommonPartition, Long] = {
    offset match {
      case o: CommonSourceOffset => o.partitionToOffsets
      case so: SerializedOffset => CommonSourceOffset(so).partitionToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to KafkaSourceOffset")
    }
  }

  /**
   * Returns [[KafkaSourceOffset]] from a variable sequence of (topic, partitionId, offset)
   * tuples.
   */
  def apply(offset: Offset): CommonSourceOffset = {
    offset match {
      case o: CommonSourceOffset => o
      case so: SerializedOffset => CommonSourceOffset(so)
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to KafkaSourceOffset")
    }
  }

  def apply(offsetTuples: (String, Int, Long)*): CommonSourceOffset = {
    CommonSourceOffset(offsetTuples.map { case (t, p, o) => (new CommonPartition(t, p), o) }.toMap)
  }

  /**
   * Returns [[KafkaSourceOffset]] from a JSON [[SerializedOffset]]
   */
  def apply(offset: SerializedOffset): CommonSourceOffset =
    CommonSourceOffset(partitionOffsets(offset.json))
}

case class CommonOffsetRange(commonPartition: CommonPartition, fromOffset: Long, untilOffset: Long)
