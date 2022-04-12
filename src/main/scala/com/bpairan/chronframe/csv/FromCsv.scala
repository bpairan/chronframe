package com.bpairan.chronframe.csv

import cats.implicits._
import net.openhft.chronicle.core.OS
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder
import net.openhft.chronicle.queue.{ChronicleQueue, ExcerptAppender}
import net.openhft.chronicle.wire.WireOut

import java.nio.file.Path
import java.time.{LocalDate, LocalDateTime}
import scala.io.Source
import scala.util.Using

/**
 * Created by Bharathi Pairan on 03/04/2022.
 */
class FromCsv(separator: Char, escape: Char) {

  def parse(inputPath: Path, queue: ChronicleQueue): Unit = {
    Using.Manager { use =>
      val source = use(Source.fromFile(inputPath.toFile))
      val iterator = source.getLines()
      CsvParser.parse(1, iterator.next(), separator, escape).map { header =>
        val appender = use(queue.acquireAppender())
        iterator.zipWithIndex.map { case (line, idx) => CsvParser.parse(idx + 2, line, separator, escape) }
          .map(line => write(header, line, appender))
          .reduce[CsvParserErrorOr[Unit]] { case (x, y) => x.combine(y) }
      }
    }
  }

  def write(header: Seq[String], errorOrLine: CsvParserErrorOr[Seq[String]], appender: ExcerptAppender): CsvParserErrorOr[Unit] = {
    errorOrLine.map { line =>
      appender.writeDocument { w =>
        line.zip(header).foldLeft(w) { case (wireOut, (value, name)) => writeTyped(wireOut, name, value) }
      }
    }
  }

  def writeTyped(wire: WireOut, column: String, value: Any): WireOut = {
    val valueOut = wire.write(column)
    value match {
      case f: Float => valueOut.float32(f)
      case d: Double => valueOut.float64(d)
      case s: Short => valueOut.int16(s)
      case i: Int => valueOut.int32(i)
      case l: Long => valueOut.int64(l)
      case b: Boolean => valueOut.bool(b)
      case ld: LocalDate => valueOut.date(ld)
      case ldt: LocalDateTime => valueOut.dateTime(ldt)
      case s: CharSequence => valueOut.text(s)
      case _@a => valueOut.text(a.asInstanceOf[String])
    }
  }
}
