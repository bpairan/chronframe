package com.bpairan.chronframe.converter

/**
 * Created by Bharathi Pairan on 13/04/2022.
 */
object ToBooleanConverter extends ValueConverter[Boolean] {
  override protected def convertValue: Convert = {
    case v => ValueConverter.processFlag(v)
  }

}
