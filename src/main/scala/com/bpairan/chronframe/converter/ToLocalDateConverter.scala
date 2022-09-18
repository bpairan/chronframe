package com.bpairan.chronframe.converter

import java.time.LocalDate
import scala.util.Try

/**
 * Created by Bharathi Pairan on 13/04/2022.
 */
object ToLocalDateConverter extends ValueConverter[LocalDate] {
  private val DefaultDatePatterns = Seq("yyyyMMdd",
    "yyyy/MM/dd",
    "yyyy-MM-dd",
    "MM/dd/yy",
    "MM-dd-yy")

  private val datePatterns = sys.props.get("chronframe.date.pattern.overrides").map(_.split(',').toSeq)
    .map(ToDateTimeFormatter).getOrElse(ToDateTimeFormatter(DefaultDatePatterns))

  override protected def convertValue: Convert = {
    case v => parseDatePattern(v)
  }

  def parseDatePattern(date: String): Option[LocalDate] = {
    Try(LocalDate.parse(date, datePatterns)).toOption
  }

}
