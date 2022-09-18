package com.bpairan.chronframe

import com.bpairan.chronframe.converter.ValueConverter
import net.openhft.chronicle.queue.ExcerptTailer

import scala.util.Try

/**
 * Created by Bharathi Pairan on 04/04/2022.
 */
class ChronFrameRow private (columnIndexes: List[ColumnIndex], renamedColumns: Map[String, String], tailer: ExcerptTailer) {

  def apply[T](columnName: String)(implicit converter: ValueConverter[T]): Option[T] = {
    valueOf(columnName)
  }

  def valueOf[T](columnName: String)(implicit converter: ValueConverter[T]): Option[T] = {
    val searchColumnName = renamedColumns.getOrElse(columnName, columnName)
    columnIndexes.find(ci => ci.column == searchColumnName)
      .flatMap { columnIndex =>
        Try(tailer.moveToIndex(columnIndex.idx)).fold(_ => throw new IllegalArgumentException(s"index not found"), identity)
        val dc = tailer.readingDocument()
        if (!dc.isPresent) throw new IllegalArgumentException(s"Document not found on queue at requested index")
        converter.convert(dc.wire().read(searchColumnName))
      }
  }

}

object ChronFrameRow {

  implicit class TailerToChronFrameRowConverter(val tailer: ExcerptTailer) extends AnyVal {
    def toChronFrameRow(columnIndexes: List[ColumnIndex], renamedColumns: Map[String, String]): ChronFrameRow = {
      new ChronFrameRow(columnIndexes, renamedColumns, tailer)
    }
  }
}
