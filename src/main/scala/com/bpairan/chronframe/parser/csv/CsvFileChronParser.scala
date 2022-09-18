package com.bpairan.chronframe.parser.csv

import com.bpairan.chronframe.parser.{ChronParser, ParserErrorOr}

import java.nio.file.Path
import scala.io.Source

/**
 * Created by Bharathi Pairan on 09/07/2022.
 */
class CsvFileChronParser(inputPath: Path, separator: Char, escape: Char) extends ChronParser with AutoCloseable {

  private val source = Source.fromFile(inputPath.toFile)
  private val iterator = source.getLines()

  override def header(): ParserErrorOr[Seq[String]] = {
    CsvParser.parse(1, iterator.next(), separator, escape)
  }

  override def rows(): Iterator[ParserErrorOr[Seq[String]]] = {
    iterator.zipWithIndex.map { case (line, idx) => CsvParser.parse(idx + 2, line, separator, escape) }
  }

  override def close(): Unit = {
    source.close()
  }
}
