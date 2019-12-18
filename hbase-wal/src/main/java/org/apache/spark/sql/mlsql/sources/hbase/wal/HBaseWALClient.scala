package org.apache.spark.sql.mlsql.sources.hbase.wal

import java.io.EOFException
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Delete, Put}
import org.apache.hadoop.hbase.wal.{WAL, WALEdit, WALFactory}
import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.sources.v2.reader.streaming.Offset
import org.apache.spark.streaming.RawEvent
import org.spark_project.guava.cache.{CacheBuilder, CacheLoader, LoadingCache}
import tech.mlsql.binlog.common.HDFSContext
import tech.mlsql.common.utils.log.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 10/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class HBaseWALClient(walLogPath: String, oldWALLogPath: String, startTime: Long, conf: Configuration) extends Logging {
  val readers = ArrayBuffer[PathAndReader]()
  val old_readers = ArrayBuffer[PathAndReader]()
  val eventListeners = ArrayBuffer[HBaseWALEventListener]()

  val walDoneFiles = CacheBuilder.newBuilder().maximumSize(100000).expireAfterWrite(60, TimeUnit.MINUTES).build[String, String](
    new CacheLoader[String, String] {
      override def load(k: String): String = WALFileStat.DEFAULT
    }
  )

  val oldWALDoneFiles = CacheBuilder.newBuilder().maximumSize(100000).expireAfterWrite(60, TimeUnit.MINUTES).build[String, String](
    new CacheLoader[String, String] {
      override def load(k: String): String = WALFileStat.DEFAULT
    }
  )

  // region dir -> wal log file
  val activeWalFiles = new mutable.HashSet[String]()

  def connect() = {
    val shouldBreak = new AtomicBoolean(false)
    while (!shouldBreak.get()) {
      activeWalFiles.clear()
      fetch()
      fetchOldWALs()
    }
  }

  private def fetchOldWALs(): Unit = {
    if (oldWALLogPath == "") return
    val walLogPathInHDFS = new Path(oldWALLogPath)
    val hdfsContext = new HDFSContext(walLogPathInHDFS, conf)

    val targetFilePathIter = hdfsContext.fc.listStatus(walLogPathInHDFS)
    val buffer = ArrayBuffer[Path]()
    while (targetFilePathIter.hasNext) {
      buffer += targetFilePathIter.next().getPath
    }
    val targetFiles = buffer.filterNot(f => f.getName.endsWith(".meta") || f.getName.endsWith(".log")).sortBy(f => f.getName)
    targetFiles.filterNot { f =>
      oldWALDoneFiles.getIfPresent(f.toString) != null
    }.foreach { targetFile =>
      try {
        val reader = WALFactory.createReader(hdfsContext.fs, targetFile, conf)
        old_readers += PathAndReader(targetFile, reader)
      } catch {
        case e: EOFException => logInfo(s"HBase OldWAL read ${targetFile} fail")
      }

    }
    iterateReadFile(old_readers, oldWALDoneFiles, false)
    old_readers.foreach(_.reader.close())
    old_readers.clear()
  }

  private def iterateReadFile(readers: ArrayBuffer[PathAndReader], walDoneFiles: LoadingCache[String, String], ignoreActiveFile: Boolean) = {
    readers.foreach { readerAndPath =>
      val shouldBreak = new AtomicBoolean(false)
      while (!shouldBreak.get()) {

        val entry = try {
          readerAndPath.reader.next()
        } catch {
          case e: EOFException =>
            logInfo(s"${readerAndPath.path} is moved ")
            null
        }


        if (entry == null) {
          shouldBreak.set(true)
          if (ignoreActiveFile && !activeWalFiles.contains(readerAndPath.path.toString)) {
            walDoneFiles.put(readerAndPath.path.toString, WALFileStat.DONE)
          }
        } else {
          eventListeners.foreach { el =>
            map(entry, (evt) => {
              //println(evt)
              el.onEvent(evt)
            })
          }

        }
      }
    }
  }

  private def fetch() = {
    val walLogPathInHDFS = new Path(walLogPath)
    val hdfsContext = new HDFSContext(walLogPathInHDFS, conf)

    val regionServerDirs = hdfsContext.fc.listStatus(walLogPathInHDFS)

    while (regionServerDirs.hasNext) {
      val path = regionServerDirs.next()
      val targetFilePathIter = hdfsContext.fc.listStatus(path.getPath)
      val buffer = ArrayBuffer[Path]()
      while (targetFilePathIter.hasNext) {
        buffer += targetFilePathIter.next().getPath
      }
      val targetFiles = buffer.filterNot(f => f.getName.endsWith(".meta")).sortBy(f => f.getName)
      if (targetFiles.size > 0) {
        val activeFile = targetFiles.last
        activeWalFiles += (activeFile.toString)
        targetFiles.filterNot { f =>
          walDoneFiles.getIfPresent(f.toString) != null
        }.foreach { targetFile =>
          try {
            val reader = WALFactory.createReader(hdfsContext.fs, targetFile, conf)
            readers += PathAndReader(path.getPath, reader)
          } catch {
            case e: EOFException => logInfo(s"HBase WAL read ${path.getPath} fail")
          }

        }
      }

    }

    iterateReadFile(readers, walDoneFiles, true)
    readers.foreach(_.reader.close())
    readers.clear()

  }

  def disConnect = {

  }

  private def map(entry: WAL.Entry, collectEvt: (Seq[RawHBaseWALEvent]) => Unit) = {
    val key = entry.getKey
    val value = entry.getEdit
    val db = key.getTableName.getNamespaceAsString
    val table = key.getTableName.getNameAsString
    val sequenceId = key.getSequenceId
    val regionName = new String(key.getEncodedRegionName, Charset.forName("utf-8"))
    val time = key.getWriteTime


    var put: Put = null
    var del: Delete = null
    var lastCell: Cell = null

    val batchBuffer = ArrayBuffer[RawHBaseWALEvent]()

    value.getCells().asScala.filterNot(WALEdit.isMetaEditFamily(_)).foreach { cell =>
      if (lastCell == null || lastCell.getTypeByte != cell.getTypeByte || !CellUtil.matchingRows(lastCell, cell)) {
        if (put != null) {
          batchBuffer += (RawHBaseWALEvent(put, null, db, table, RawHBaseEventOffset(regionName, sequenceId), time))

        }
        if (del != null) {
          batchBuffer += (RawHBaseWALEvent(null, del, db, table, RawHBaseEventOffset(regionName, sequenceId), time))
        }
        if (CellUtil.isDelete(cell)) {
          del = new Delete(CellUtil.cloneRow(cell))
        } else {
          put = new Put(CellUtil.cloneRow(cell))
        }
      }
      if (CellUtil.isDelete(cell)) {
        del.add(cell)
      } else {
        put.add(cell)
      }
      lastCell = cell
    }
    if (put != null) {
      batchBuffer += (RawHBaseWALEvent(put, null, db, table, RawHBaseEventOffset(regionName, sequenceId), time))
    }
    if (del != null) {
      batchBuffer += (RawHBaseWALEvent(null, del, db, table, RawHBaseEventOffset(regionName, sequenceId), time))
    }
    collectEvt(batchBuffer.toSeq)

  }

  def register(eventListener: HBaseWALEventListener): Unit = {
    eventListeners += eventListener
  }

}

object WALFileStat {
  val DONE = "done"
  val DEFAULT = "default"
}

trait HBaseWALEventListener {
  def onEvent(event: Seq[RawHBaseWALEvent]): Unit
}

case class PathAndReader(path: Path, reader: WAL.Reader)

case class RawHBaseWALEvent(put: Put, del: Delete, db: String, table: String, offset: RawHBaseEventOffset, time: Long) extends RawEvent {

  override def key(): String = offset.regionName

  override def pos(): Offset = LongOffset(offset.sequenceId)
}

case class RawHBaseWALEventsSerialization(_key: String, _pos: Offset, item: Seq[String]) extends RawEvent {
  override def key(): String = _key

  override def pos(): Offset = _pos
}

case class RawHBaseEventOffset(regionName: String, sequenceId: Long)
