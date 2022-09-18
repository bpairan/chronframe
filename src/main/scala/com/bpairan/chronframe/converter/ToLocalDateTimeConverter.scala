package com.bpairan.chronframe.converter

import java.time.LocalDateTime

/**
 * Created by Bharathi Pairan on 13/04/2022.
 */
object ToLocalDateTimeConverter extends ValueConverter[LocalDateTime] {

  override protected def convertValue: ToLocalDateTimeConverter.Convert = {
    case v => parseTime(v)
  }
}
