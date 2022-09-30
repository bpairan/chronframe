package com.bpairan.chronframe

import com.bpairan.chronframe.ChronFrameRow.TailerToChronFrameRowConverter
import com.bpairan.chronframe.parser.WriteData.WireOutOps
import com.bpairan.chronframe.parser.csv.CsvFileChronParser
import com.bpairan.chronframe.parser.{ParserErrorOr, WriteData}
import net.openhft.chronicle.core.OS
import net.openhft.chronicle.core.io.IOTools
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder
import net.openhft.chronicle.queue.{ChronicleQueue, ExcerptAppender, ExcerptTailer}

import java.nio.file.Path
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Created by Bharathi Pairan on 04/04/2022.
 */
case class ChronFrame private(columnIndexes: List[List[ColumnIndex]], queue: ChronicleQueue) extends IndexedSeq[ChronFrameRow] with AutoCloseable {
  private val tailer: ExcerptTailer = queue.createTailer()
  private val appender: ExcerptAppender = queue.acquireAppender()
  private val renamedColumns: mutable.Map[String, String] = mutable.Map()

  override def apply(i: Int): ChronFrameRow = {
    // Need to move across all indexes and combine data without memory copying, is it possible?
    Try(columnIndexes(i)).fold(_ => throw new IllegalArgumentException(s"index: $i not found"), columnIndexes => tailer.toChronFrameRow(columnIndexes, renamedColumns.map(_.swap).toMap))
  }

  // Get number of indexes from the head
  override def length: Int = {
    columnIndexes.size
  }

  def columns: Seq[String] = columnIndexes.headOption.map(list => list.map(ci => ci.column)).getOrElse(Seq.empty)

  def listOfColumns: java.util.List[String] = columns.asJava

  /**
   * Renames existing column in [[columnIndexes]]
   *
   * @param existingName current column name
   * @param newName      new column name
   * @return [[ChronFrame]]
   */
  def rename(existingName: String, newName: String): ChronFrame = {
    renamedColumns += existingName -> newName
    this
  }

  /**
   * Renames existing columns in [[columnIndexes]]
   *
   * @param nameMap Map of existing column name to new column name
   * @return [[ChronFrame]]
   */
  def rename(nameMap: Map[String, String]): ChronFrame = {
    nameMap.foreach { case (key, value) => renamedColumns += key -> value }
    this
  }

  def rename(nameMap: java.util.Map[String, String]): ChronFrame = {
    rename(nameMap.asScala.toMap)
  }

  /**
   * Drops the requested columns from all the rows in [[columnIndexes]], does not remove the columns from the underlying storage
   *
   * @param columns columns to drop
   * @return [[ChronFrame]]
   */
  def drop(columns: String*): ChronFrame = {
    val newIndexes = columnIndexes.map(columnIndexes => columnIndexes.filterNot(ci => columns.contains(ci.column)))
    this.copy(columnIndexes = newIndexes)
  }

  def drop(columns: java.util.List[String]): ChronFrame = {
    drop(columns.asScala.toSeq: _*)
  }

  /**
   * Appends column to ChronFrame, Size of data should be equal to the size of columnIndexes
   * i.e data should be inserted for all rows
   */
  def appendColumn(columnName: String, data: Seq[Any]): ChronFrame = {
    if (data.size != columnIndexes.size) throw new IllegalArgumentException("size of data should be same as size of ChronFrame")

    val appendedColumnIndexes = data.zip(columnIndexes).foldLeft(List[List[ColumnIndex]]()) { case (result, (d, colIdxs: Seq[ColumnIndex])) =>
      appender.writeDocument(w => w.write(columnName, d.toString))
      val index = appender.lastIndexAppended()
      result :+ (colIdxs :+ ColumnIndex(index, columnName))
    }
    this.copy(columnIndexes = appendedColumnIndexes)
  }

  def appendColumn(columnName: String, data: java.util.List[Object]): ChronFrame = {
    appendColumn(columnName, data.asScala.toSeq)
  }

  /**
   * Add new row to the Frame
   *
   * @param data columns and values in a Map
   * @return [[ChronFrame]]
   */
  def addRow(data: Map[String, Any]): ChronFrame = {
    this.copy(columnIndexes = columnIndexes :+ _addRow(data))
  }

  def addRow(data: java.util.Map[String, Object]): ChronFrame = {
    addRow(data.asScala.toMap)
  }

  def addRows(iterator: Iterable[Map[String, Any]]): ChronFrame = {
    val newRows = iterator.foldLeft(List[List[ColumnIndex]]()) { case (result, data) => result :+ _addRow(data) }
    this.copy(columnIndexes = columnIndexes ++ newRows)
  }

  def addRows(list: java.util.List[java.util.Map[String, Object]]): ChronFrame = {
    addRows(list.asScala.map(_.asScala.toMap))
  }

  private def _addRow(data: Map[String, Any]): List[ColumnIndex] = {
    val header = data.keys.toSeq
    appender.writeDocument(w => w.toDocument(data.values.map(_.toString).toSeq, header))
    val index = appender.lastIndexAppended()
    header.map(col => ColumnIndex(index, col)).toList
  }

  override def close(): Unit = {
    tailer.close()
    queue.close()
    IOTools.shallowDeleteDirWithFiles(s"${queue.fileAbsolutePath()}")
  }

}

object ChronFrame {

  def instance: ChronFrame.type = this

  def newChronicleQueue(name: String, path: Option[Path] = None): ChronicleQueue = {
    val basePath = path.map(_.toString).getOrElse(s"${OS.getTarget}/$name")
    SingleChronicleQueueBuilder.single(basePath).build()
  }

  def fromCsv(input: Path, output: Option[Path], separator: Char, escape: Char): ParserErrorOr[ChronFrame] = {
    val queue = newChronicleQueue(input.getFileName.toString, output)
    fromCsv(input, queue, separator, escape)
  }

  def fromCsv(input: Path, queue: ChronicleQueue, separator: Char, escape: Char): ParserErrorOr[ChronFrame] = {
    val parser = new CsvFileChronParser(input, separator, escape)
    WriteData.from(parser, queue).map(new ChronFrame(_, queue))
  }
}
