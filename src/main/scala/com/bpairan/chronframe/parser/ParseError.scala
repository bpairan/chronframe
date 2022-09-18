package com.bpairan.chronframe.parser

/**
 * Created by Bharathi Pairan on 03/04/2022.
 */
abstract class ParseError(val message: String)

case class SourceNotFound(override val message: String) extends ParseError(message)

case class LineParseError(idx: Int, override val message: String) extends ParseError(message)

case class SourceParseError(override val message: String) extends ParseError(message)

