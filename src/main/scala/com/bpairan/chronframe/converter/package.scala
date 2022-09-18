package com.bpairan.chronframe

import com.bpairan.chronframe.converter.Ordering.CaseInsensitive

import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import scala.collection.immutable.TreeSet
import scala.util.Try

/**
 * Created by Bharathi Pairan on 13/04/2022.
 */
package object converter {

  val SpecialStringValues: Set[String] = TreeSet("nan", "infinity", "-infinity", "inf", "-inf")(CaseInsensitive)

  private val DefaultDateTimeFormatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .appendOptional(DateTimeFormatter.ISO_DATE_TIME)
    .appendOptional(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    .appendOptional(DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"))
    .appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    .appendOptional(DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss"))
    .appendOptional(DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"))
    .appendOptional(DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"))
    .toFormatter()

  val ToDateTimeFormatter: Seq[String] => DateTimeFormatter = (overrides: Seq[String]) =>
    overrides.foldLeft(new DateTimeFormatterBuilder()) { case (builder, pattern) => builder.appendOptional(DateTimeFormatter.ofPattern(pattern)) }.toFormatter()

  val DateTimePatterns: DateTimeFormatter = sys.props.get("chronframe.date.time.pattern.overrides").map(_.split(',').toSeq).map(ToDateTimeFormatter).getOrElse(DefaultDateTimeFormatter)


  def parseTime(value: String): Option[LocalDateTime] = {
    Try(LocalDateTime.parse(value, DateTimePatterns)).toOption
  }

  def parseStringForFloats[T](str: String, parseFn: String => T): Option[T] = {
    if (SpecialStringValues.contains(str)) {
      None
    } else {
      Try(parseFn(str)).toOption
    }
  }
}
