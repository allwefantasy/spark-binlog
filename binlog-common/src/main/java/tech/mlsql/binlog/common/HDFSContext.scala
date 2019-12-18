package tech.mlsql.binlog.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}

/**
 * 10/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class HDFSContext(path: Path, conf: Configuration) {
  val fs = path.getFileSystem(conf)
  val fc = FileContext.getFileContext(path.toUri, conf)
}
