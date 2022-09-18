package com.bpairan.chronframe.parser.csv

import com.bpairan.chronframe.parser.{LineParseError, ParserErrorOr}

import scala.collection.mutable
import cats.implicits._

/**
 * Created by Bharathi Pairan on 03/04/2022.
 */
object CsvParser {

  def parse(idx: Int, line: String, separator: Char, escape: Char): ParserErrorOr[Seq[String]] = {
    var startQuote = false
    var endQuote = false
    val builder = new mutable.StringBuilder(line.length)
    val result = new mutable.ArrayBuffer[String](1024)
    try {
      for (c <- line) {
        c match {
          case e@_ if e == escape && !startQuote => startQuote = true
          case e@_ if e == escape && startQuote => endQuote = true
          case s@_ if s == separator && ((startQuote && endQuote) || (!startQuote && !endQuote)) =>
            result += builder.toString().trim
            builder.clear()
            startQuote = false
            endQuote = false
          case s@_ if s == separator && startQuote && !endQuote => builder.append(separator)
          case i@_ => builder.append(i)
        }
      }
      // add the last column
      val lastCol = builder.toString().trim
      if (lastCol.nonEmpty) {
        val lastSeparatorIdx = lastCol.lastIndexOf(separator)

        if (lastSeparatorIdx != -1 && lastSeparatorIdx == lastCol.length - 1) {
          result += lastCol.substring(0, lastSeparatorIdx)
        } else {
          result += lastCol
        }
      }
      result.toSeq.validNel
    } catch {
      case t: Throwable => LineParseError(idx, t.getMessage).invalidNel
    }
  }
}
