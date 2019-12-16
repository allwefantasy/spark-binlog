package tech.mlsql.binlog.common

import org.apache.spark.{SparkEnv, SparkException}
import tech.mlsql.common.utils.distribute.socket.server.ReportHostAndPort
import tech.mlsql.common.utils.log.Logging

/**
 * 12/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
object ConsumerCache extends Logging {

  private case class CacheKey(host: String, port: Int)

  private lazy val cache = {
    val conf = SparkEnv.get.conf
    val capacity = conf.getInt("spark.sql.mlsql.binlog.capacity", 1024)
    new java.util.LinkedHashMap[CacheKey, BinlogConsumer](capacity, 0.75f, true) {
      override def removeEldestEntry(
                                      entry: java.util.Map.Entry[CacheKey, BinlogConsumer]): Boolean = {

        // Try to remove the least-used entry if its currently not in use.
        //
        // If you cannot remove it, then the cache will keep growing. In the worst case,
        // the cache will grow to the max number of concurrent tasks that can run in the executor,
        // (that is, number of tasks slots) after which it will never reduce. This is unlikely to
        // be a serious problem because an executor with more than 64 (default) tasks slots is
        // likely running on a beefy machine that can handle a large number of simultaneously
        // active consumers.

        if (!entry.getValue.inUse && this.size > capacity) {
          logWarning(
            s"BinlogConsumer cache hitting max capacity of $capacity, " +
              s"removing consumer for ${entry.getKey}")
          try {
            entry.getValue.close
          } catch {
            case e: SparkException =>
              logError(s"Error closing earliest binlog consumer for ${entry.getKey}", e)
          }
          true
        } else {
          false
        }
      }
    }
  }

  def acquire(hostAndPort: ReportHostAndPort, createConsumerInstance: () => BinlogConsumer): BinlogConsumer = synchronized {
    val key = new CacheKey(hostAndPort.host, hostAndPort.port)
    val existingInternalConsumer = cache.get(key)

    lazy val newInternalConsumer = createConsumerInstance()

    if (existingInternalConsumer == null) {
      // If consumer is not already cached, then put a new in the cache and return it
      cache.put(key, newInternalConsumer)
      newInternalConsumer.markInUse
      newInternalConsumer

    } else if (existingInternalConsumer.inUse) {
      // If consumer is already cached but is currently in use, then return a new consumer
      newInternalConsumer

    } else {
      // If consumer is already cached and is currently not in use, then return that consumer
      existingInternalConsumer.markInUse
      existingInternalConsumer
    }
  }

  def release(intConsumer: BinlogConsumer): Unit = {
    synchronized {

      // Clear the consumer from the cache if this is indeed the consumer present in the cache
      val key = new CacheKey(intConsumer.targetHostAndPort.host,
        intConsumer.targetHostAndPort.port)

      val cachedIntConsumer = cache.get(key)
      if (intConsumer.eq(cachedIntConsumer)) {
        // The released consumer is the same object as the cached one.
        if (intConsumer.isClose) {
          intConsumer.close
          cache.remove(key)
        } else {
          intConsumer.markInIdle
        }
      } else {
        // The released consumer is either not the same one as in the cache, or not in the cache
        // at all. This may happen if the cache was invalidate while this consumer was being used.
        // Just close this consumer.
        intConsumer.close
        logInfo(s"Released a supposedly cached consumer that was not found in the cache")
      }
    }
  }
}
