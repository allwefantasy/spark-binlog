package org.apache.spark.sql.mlsql.sources.hbase.wal

import java.io.EOFException
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Delete, Put}
import org.apache.hadoop.hbase.wal.{WAL, WALEdit, WALFactory}
import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.sources.v2.reader.streaming.Offset
import org.apache.spark.streaming.RawEvent
import tech.mlsql.binlog.common.HDFSContext
import tech.mlsql.common.utils.log.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * 10/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class HBaseWALClient(walLogPath: String, startTime: Long, conf: Configuration) extends Logging {
  val readers = ArrayBuffer[PathAndReader]()
  val eventListeners = ArrayBuffer[HBaseWALEventListener]()

  def connect() = {
    val shouldBreak = new AtomicBoolean(false)
    while (!shouldBreak.get()) {
      fetch()
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
      val targetFile = buffer.filterNot(f => f.getName.endsWith(".meta")).head
      val reader = WALFactory.createReader(hdfsContext.fs, targetFile, conf)
      readers += PathAndReader(path.getPath, reader)
    }

    readers.foreach { readerAndPath =>
      val shouldBreak = new AtomicBoolean(false)
      while (!shouldBreak.get()) {

        val entry = try {
          readerAndPath.reader.next()
        } catch {
          case e: EOFException =>
            logInfo(s"${readerAndPath.path} is moved to oldWALs")
            //todo: handle the situation hbase wal log moved to oldWALs
            null
        }


        if (entry == null) {
          shouldBreak.set(true)
        } else {
          eventListeners.foreach { el =>
            map(entry, (evt) => {
              el.onEvent(evt)
            })
          }

        }
      }
    }
    readers.foreach(_.reader.close())
    readers.clear()

  }

  def disConnect = {

  }

  private def map(entry: WAL.Entry, collectEvt: (RawHBaseWALEvent) => Unit) = {
    val key = entry.getKey
    val value = entry.getEdit
    val db = key.getTableName.getNameAsString
    val table = key.getTableName.getQualifierAsString
    val sequenceId = key.getSequenceId
    val regionName = new String(key.getEncodedRegionName, Charset.forName("utf-8"))
    val time = key.getWriteTime


    var put: Put = null
    var del: Delete = null
    var lastCell: Cell = null
    value.getCells().asScala.filterNot(WALEdit.isMetaEditFamily(_)).foreach { cell =>
      if (lastCell == null || lastCell.getTypeByte != cell.getTypeByte || !CellUtil.matchingRows(lastCell, cell)) {
        if (put != null) {
          collectEvt(RawHBaseWALEvent(put, null, db, table, RawHBaseEventOffset(regionName, sequenceId), time))

        }
        if (del != null) {
          collectEvt(RawHBaseWALEvent(null, del, db, table, RawHBaseEventOffset(regionName, sequenceId), time))
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
      collectEvt(RawHBaseWALEvent(put, null, db, table, RawHBaseEventOffset(regionName, sequenceId), time))

    }
    if (del != null) {
      collectEvt(RawHBaseWALEvent(null, del, db, table, RawHBaseEventOffset(regionName, sequenceId), time))
    }

  }

  def register(eventListener: HBaseWALEventListener): Unit = {
    eventListeners += eventListener
  }

}

trait HBaseWALEventListener {
  def onEvent(event: RawHBaseWALEvent): Unit
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
