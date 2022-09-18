package com.bpairan.chronframe.converter

import java.sql.Date

/**
 * Created by Bharathi Pairan on 13/04/2022.
 */
object ToSqlDateConverter extends ValueConverter[Date] {

  override protected def convertValue: Convert = {
    case v => ToLocalDateConverter.parseDatePattern(v).map(Date.valueOf)
  }

}
