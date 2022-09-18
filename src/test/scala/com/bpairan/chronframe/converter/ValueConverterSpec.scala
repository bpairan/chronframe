package com.bpairan.chronframe.converter

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Created by Bharathi Pairan on 25/06/2022.
 */
class ValueConverterSpec extends AnyFlatSpec with Matchers {

  "processFlag" should "be None" in  {
    ValueConverter.processFlag(null) shouldBe None
  }
}
