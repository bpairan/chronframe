package com.bpairan.chronframe.converter

import com.google.common.base.CharMatcher._

/**
 * Created by Bharathi Pairan on 13/04/2022.
 */
object ToStringConverter extends ValueConverter[String] {
  private val CharMatcher = javaIsoControl().and(anyOf("\r\n\t").negate())

  override protected def convertValue: Convert = {
    case v =>
      Option(CharMatcher.removeFrom(v.trim))
  }

}
