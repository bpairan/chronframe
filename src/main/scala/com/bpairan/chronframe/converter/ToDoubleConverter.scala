package com.bpairan.chronframe.converter

/**
 * Created by Bharathi Pairan on 13/04/2022.
 */
object ToDoubleConverter extends ValueConverter[Double] {
  override protected def convertValue: Convert = {
    case v => parseStringForFloats(v, str => str.replaceAll("[,L]", "").toDouble)
  }
}
