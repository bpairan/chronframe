package com.bpairan.chronframe.parser

/**
 * Created by Bharathi Pairan on 09/07/2022.
 */
trait ChronParser {
  def header(): ParserErrorOr[Seq[String]]

  def rows(): Iterator[ParserErrorOr[Seq[String]]]
}
