package org.apache.spark.sql.mlsql.sources.hbase.wal

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.wal.{WAL, WALFactory}
import tech.mlsql.binlog.common.HDFSContext

import scala.collection.mutable.ArrayBuffer

/**
 * 10/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class HBaseWALClient(walLogPath: String, startTime: Long, conf: Configuration) {
  val readers = ArrayBuffer[PathAndReader]()
  val eventListeners = ArrayBuffer[HBaseWALEventListener]()

  def connect() = {
    val shouldBreak = new AtomicBoolean(false)
    while (!shouldBreak.get()) {
      //todo: how to handle hbase move WAL to oldWAls?
      fetch()
    }
  }

  private def fetch() = {
    val walLogPathInHDFS = new Path(walLogPath)
    val hdfsContext = new HDFSContext(walLogPathInHDFS, conf)

    val regionServerDirs = hdfsContext.fc.listStatus(walLogPathInHDFS)

    while (regionServerDirs.hasNext) {
      val path = regionServerDirs.next()
      val reader = WALFactory.createReader(hdfsContext.fs, walLogPathInHDFS, conf)
      readers += PathAndReader(path.getPath, reader)
    }

    readers.foreach { readerAndPath =>
      val shouldBreak = new AtomicBoolean(false)
      while (!shouldBreak.get()) {
        val entry = readerAndPath.reader.next()
        if (entry == null) {
          shouldBreak.set(true)
        }
        eventListeners.foreach { el =>
          el.onEvent(entry)
        }
      }
      readerAndPath.reader.close()
    }

  }

  def register(eventListener: HBaseWALEventListener): Unit = {
    eventListeners += eventListener
  }

}

trait HBaseWALEventListener {
  def onEvent(event: WAL.Entry): Unit
}

case class PathAndReader(path: Path, reader: WAL.Reader)
