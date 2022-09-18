package com.bpairan.chronframe.converter

import scala.util.Try

/**
 * Created by Bharathi Pairan on 13/04/2022.
 */
object ToIntConverter extends ValueConverter[Int] {
  override protected def convertValue: Convert = {
    case v => Try(v.replaceAll(",", "").toInt).toOption
  }
}
