package com.bpairan.chronframe.csv

/**
 * Created by Bharathi Pairan on 03/04/2022.
 */
sealed abstract class CsvParseError(val message: String)

case class FileNotFound(override val message: String) extends CsvParseError(message)

case class FileParseError(idx: Int, override val message: String) extends CsvParseError(message)

case class InvalidParameter(override val message: String) extends CsvParseError(message)
