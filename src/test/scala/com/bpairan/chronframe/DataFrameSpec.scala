package com.bpairan.chronframe

import cats.implicits._
import net.openhft.chronicle.core.OS
import net.openhft.chronicle.core.io.IOTools
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Created by Bharathi Pairan on 04/04/2022.
 */
class DataFrameSpec extends AnyFlatSpec with Matchers {

  it should "return length" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = DataFrame.newChronicleQueue("test")
    val df = DataFrame.fromCsv(inputPath, queue, ',', '"')
    df.length shouldBe 2
    IOTools.shallowDeleteDirWithFiles(s"${OS.getTarget}/test")
  }

  it should "get first document" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = DataFrame.newChronicleQueue("test")
    private val df = DataFrame.fromCsv(inputPath, queue, ',', '"')

    // head
    private val dc0 = df(0)
    private val name0: String = dc0.wire().read("name").text()
    name0 shouldBe "Emily"
    println(s"name:$name0")

    // second
    private val dc1 = df(1)
    private val name1: String = dc1.wire().read("name").text()
    name1 shouldBe "Thomas"

    IOTools.shallowDeleteDirWithFiles(s"${OS.getTarget}/test")
  }

  it should "throw exception" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = DataFrame.newChronicleQueue("test")
    private val df = DataFrame.fromCsv(inputPath, queue, ',', '"')

    the[IllegalArgumentException] thrownBy df(2) should have message "index: 2 not found"

    IOTools.shallowDeleteDirWithFiles(s"${OS.getTarget}/test")
  }

  "slice" should "get first item from iterator" in {
    val marksArray = Array(56, 66, 76, 86, 96)

    val idx = 4
    //    marksArray.slice(0, 1).headOption shouldBe 56.some
    val value1: Iterator[Array[Int]] = Iterator.continually(marksArray).takeWhile(_.nonEmpty)
    value1.flatten.drop(0).take(1).toList.headOption shouldBe 56.some
    value1.flatten.drop(1).take(1).toList.headOption shouldBe 66.some
    value1.flatten.drop(2).take(1).toList.headOption shouldBe 76.some
    value1.flatten.drop(3).take(1).toList.headOption shouldBe 86.some
    value1.flatten.drop(4).take(1).toList.headOption shouldBe 96.some
    // this fails
    // value1.flatten.drop(5).take(1).toList.headOption shouldBe None
  }

}
