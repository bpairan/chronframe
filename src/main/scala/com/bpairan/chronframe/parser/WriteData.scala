package com.bpairan.chronframe.parser

import cats.data.Validated
import cats.implicits._
import com.bpairan.chronframe.ColumnIndex
import net.openhft.chronicle.bytes.NoBytesStore
import net.openhft.chronicle.queue.{ChronicleQueue, ExcerptAppender}
import net.openhft.chronicle.wire.WireOut
import org.apache.commons.lang3.StringUtils

import scala.util.Using

/**
 * Created by Bharathi Pairan on 14/07/2022.
 */
object WriteData {

  val NullBytes: Array[Byte] = Int.MinValue.toString.getBytes()

  def from(parser: ChronParser, queue: ChronicleQueue): ParserErrorOr[List[List[ColumnIndex]]] = {
    Using.Manager { use =>
      parser.header() match {
        case Validated.Valid(header) => val appender = use(queue.acquireAppender())
          parser.rows().map(line => write(header, line, appender)).toList.sequence
        case Validated.Invalid(e) => e.invalid
      }
    }.fold(t => SourceParseError(t.getMessage).invalidNel, identity)
  }

  private def write(header: Seq[String],
                    errorOrLine: ParserErrorOr[Seq[String]],
                    appender: ExcerptAppender): ParserErrorOr[List[ColumnIndex]] = {
    errorOrLine.map { line =>
      appender.writeDocument(w => w.toDocument(line, header))
      val index = appender.lastIndexAppended()
      header.map(col => ColumnIndex(index, col)).toList
    }
  }

  implicit class WireOutOps(val wire: WireOut) extends AnyVal {
    def write(key: String, value: String): WireOut = {
      if (StringUtils.isEmpty(value)) {
        wire.write(key).bytes(NoBytesStore.NO_BYTES)
      } else {
        wire.write(key).bytes(value.getBytes())
      }
    }

    def toDocument(line: Seq[String], header: Seq[String]): WireOut = {
      line.zip(header).foldLeft(wire) { case (wireOut, (value, key)) => wireOut.write(key, value) }
    }
  }

}

