package com.bpairan.chronframe.converter

import java.sql.Timestamp

/**
 * Created by Bharathi Pairan on 13/04/2022.
 */
object ToTimestampConverter extends ValueConverter[Timestamp] {

  override protected def convertValue: ToTimestampConverter.Convert = {
    case v => parseTime(v).map(Timestamp.valueOf)
  }
}
