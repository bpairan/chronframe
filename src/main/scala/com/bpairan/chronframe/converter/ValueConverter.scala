package com.bpairan.chronframe.converter

import com.bpairan.chronframe.converter.Ordering.CaseInsensitive
import net.openhft.chronicle.bytes.Bytes
import net.openhft.chronicle.wire.ValueIn

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}
import scala.collection.immutable.TreeSet
import scala.util.Try

/**
 * Created by Bharathi Pairan on 13/04/2022.
 */
trait ValueConverter[T] {
  type BytesArray = Bytes[Array[Byte]]

  type Convert = PartialFunction[String, Option[T]]

  private val NaValues = TreeSet("null", "na", "n/a")(CaseInsensitive)

  private val isNa: Convert = {
    case v if NaValues.contains(v) => None
  }

  protected def convertValue: Convert

  def convert(valueIn: ValueIn): Option[T] = {
    Try(valueIn.bytes()).toOption.filter(_ != null).filter(_.nonEmpty).flatMap { bytes =>
      val b = Bytes.wrapForRead(bytes)
      val str = b.toString
      val fn = isNa orElse convertValue
      fn(str)
    }
  }

}

object ValueConverter {
  implicit val toBooleanConverter: ValueConverter[Boolean] = ToBooleanConverter
  implicit val toStringConverter: ValueConverter[String] = ToStringConverter
  implicit val toIntConverter: ValueConverter[Int] = ToIntConverter
  implicit val toLongConverter: ValueConverter[Long] = ToLongConverter
  implicit val toFloatConverter: ValueConverter[Float] = ToFloatConverter
  implicit val toDoubleConverter: ValueConverter[Double] = ToDoubleConverter
  implicit val toSqlDateConverter: ValueConverter[Date] = ToSqlDateConverter
  implicit val toLocalDateConverter: ValueConverter[LocalDate] = ToLocalDateConverter
  implicit val toLocalDateTimeConverter: ValueConverter[LocalDateTime] = ToLocalDateTimeConverter
  implicit val toTimestampConverter: ValueConverter[Timestamp] = ToTimestampConverter

  private val TrueValues = TreeSet("y", "yes", "1", "true")(CaseInsensitive)
  private val FalseValues = TreeSet("n", "no", "0", "false")(CaseInsensitive)

  def processFlag(value: String): Option[Boolean] = {
    Option(value) match {
      case Some(boolVal) if TrueValues.contains(boolVal) => Some(true)
      case Some(boolVal) if FalseValues.contains(boolVal) => Some(false)
      case _ => None
    }

  }
}
