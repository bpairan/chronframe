package com.bpairan.chronframe.converter

/**
 * Created by Bharathi Pairan on 13/04/2022.
 */
object ToFloatConverter extends ValueConverter[Float] {

  override protected def convertValue: Convert = {
    case v => parseStringForFloats(v, _.replaceAll(",", "").toFloat)
  }
}
