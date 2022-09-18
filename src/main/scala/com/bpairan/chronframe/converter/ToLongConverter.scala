package com.bpairan.chronframe.converter

import scala.util.Try

/**
 * Created by Bharathi Pairan on 13/04/2022.
 */
object ToLongConverter extends ValueConverter[Long] {
  override protected def convertValue: Convert = {
    case v: String => Try(v.replaceAll("[,L]", "").toLong).toOption
  }

}
