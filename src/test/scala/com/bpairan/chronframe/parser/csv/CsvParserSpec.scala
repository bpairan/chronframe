package com.bpairan.chronframe.parser.csv

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import cats.implicits._
/**
 * Created by Bharathi Pairan on 15/07/2022.
 */
class CsvParserSpec extends AnyFlatSpec with Matchers {

  it should "parse comma separated string" in {
    val line = "a,b,c"
    CsvParser.parse(1, line, ',', '"') shouldBe Seq("a", "b", "c").validNel
  }

  it should "parse quoted strings" in {
    val line = """"a","b","c""""
    CsvParser.parse(1, line, ',', '"') shouldBe Seq("a", "b", "c").validNel
  }

}
