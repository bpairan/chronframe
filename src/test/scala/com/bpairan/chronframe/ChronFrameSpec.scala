package com.bpairan.chronframe

import cats.implicits._
import com.bpairan.chronframe.test.TestUtils
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Created by Bharathi Pairan on 04/04/2022.
 */
class ChronFrameSpec extends AnyFlatSpec with Matchers with TestUtils with OptionValues {

  it should "return length" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    val df: ChronFrame = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value
    df.length shouldBe 2
    df.close()
  }

  it should "get first document" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    private val df = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value

    // head
    private val row0 = df(0)
    private val name0 = row0.valueOf[String]("name")
    name0 shouldBe "Emily".some
    row0.valueOf[Int]("age") shouldBe 33.some
    row0.valueOf[Int]("height") shouldBe 169.some
    row0.valueOf[String]("city") shouldBe "London".some

    df.close()
  }

  it should "read null" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    private val df = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value

    private val row = df(1)
    row.valueOf[String]("name") shouldBe "Thomas".some
    row.valueOf[Int]("age") shouldBe 25.some
    row.valueOf[Int]("height") shouldBe None
    row.valueOf[String]("city") shouldBe None

    df.close()
  }

  it should "throw exception" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    private val df = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value

    the[IllegalArgumentException] thrownBy df(2) should have message "index: 2 not found"

    df.close()
  }

  "rename" should "rename existing column" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    var df: ChronFrame = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value

    df = df.rename("city", "place")

    val row: ChronFrameRow = df(0)
    row.valueOf[String]("place") shouldBe "London".some

    df.close()
  }

  it should "rename multiple columns" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    var df: ChronFrame = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value

    df = df.rename(Map("height" -> "height_cm", "city" -> "place"))

    val row: ChronFrameRow = df(0)
    row.valueOf[Int]("height_cm") shouldBe 169.some
    row.valueOf[String]("place") shouldBe "London".some

    df.close()
  }

  "drop" should "drop the columns from frame" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    var df: ChronFrame = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value

    df = df.drop("height", "city")

    val row1: ChronFrameRow = df(0)
    row1.valueOf[String]("name") shouldBe "Emily".some
    row1.valueOf[Int]("age") shouldBe 33.some
    row1.valueOf[String]("height") shouldBe None
    row1.valueOf[String]("city") shouldBe None

    val row2: ChronFrameRow = df(1)
    row2.valueOf[String]("name") shouldBe "Thomas".some
    row2.valueOf[Int]("age") shouldBe 25.some
    row2.valueOf[String]("height") shouldBe None
    row2.valueOf[String]("city") shouldBe None

    df.close()
  }

  "addRow" should "add new row" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    var df: ChronFrame = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value

    df = df.addRow(Map("name" -> "X", "age" -> 43, "height" -> 180, "city" -> "Yard"))

    val row1: ChronFrameRow = df(0)
    row1.valueOf[String]("name") shouldBe "Emily".some
    row1.valueOf[Int]("age") shouldBe 33.some
    row1.valueOf[Int]("height") shouldBe 169.some
    row1.valueOf[String]("city") shouldBe "London".some

    val row2: ChronFrameRow = df(1)
    row2.valueOf[String]("name") shouldBe "Thomas".some
    row2.valueOf[Int]("age") shouldBe 25.some
    row2.valueOf[String]("height") shouldBe None
    row2.valueOf[String]("city") shouldBe None

    val row3: ChronFrameRow = df(2)
    row3.valueOf[String]("name") shouldBe "X".some
    row3.valueOf[Int]("age") shouldBe 43.some
    row3.valueOf[Int]("height") shouldBe 180.some
    row3.valueOf[String]("city") shouldBe "Yard".some

    df.close()
  }

  "addRows" should "add all rows" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    var df: ChronFrame = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value

    private val data = List(
      Map("name" -> "X", "age" -> 43, "height" -> 180, "city" -> "Yard"),
      Map("name" -> "Y", "age" -> 13, "height" -> 160, "city" -> "Boat")
    )

    df = df.addRows(data)

    val row1: ChronFrameRow = df(0)
    row1.valueOf[String]("name") shouldBe "Emily".some
    row1.valueOf[Int]("age") shouldBe 33.some
    row1.valueOf[Int]("height") shouldBe 169.some
    row1.valueOf[String]("city") shouldBe "London".some

    val row2: ChronFrameRow = df(1)
    row2.valueOf[String]("name") shouldBe "Thomas".some
    row2.valueOf[Int]("age") shouldBe 25.some
    row2.valueOf[String]("height") shouldBe None
    row2.valueOf[String]("city") shouldBe None

    val row3: ChronFrameRow = df(2)
    row3.valueOf[String]("name") shouldBe "X".some
    row3.valueOf[Int]("age") shouldBe 43.some
    row3.valueOf[Int]("height") shouldBe 180.some
    row3.valueOf[String]("city") shouldBe "Yard".some

    val row4: ChronFrameRow = df(3)
    row4[String]("name") shouldBe "Y".some
    row4[Int]("age") shouldBe 13.some
    row4[Int]("height") shouldBe 160.some
    row4[String]("city") shouldBe "Boat".some

    df.close()
  }

  "appendColumn" should "throw exception when size of data is not same as size of ChronFrame" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    val df: ChronFrame = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value

    val exception: IllegalArgumentException = intercept[IllegalArgumentException] {
      df.appendColumn("country", Seq("UK"))
    }
    exception.getMessage shouldBe "size of data should be same as size of ChronFrame"

    df.close()
  }

  it should "append new column" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    val df: ChronFrame = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.map(_.appendColumn("country", Seq("UK", "France"))).value

    val row1: ChronFrameRow = df(0)
    row1.valueOf[String]("name") shouldBe "Emily".some
    row1.valueOf[Int]("age") shouldBe 33.some
    row1.valueOf[Int]("height") shouldBe 169.some
    row1.valueOf[String]("city") shouldBe "London".some
    row1.valueOf[String]("country") shouldBe "UK".some

    val row2: ChronFrameRow = df(1)
    row2.valueOf[String]("name") shouldBe "Thomas".some
    row2.valueOf[Int]("age") shouldBe 25.some
    row2.valueOf[String]("height") shouldBe None
    row2.valueOf[String]("city") shouldBe None
    row2.valueOf[String]("country") shouldBe "France".some

    df.close()
  }

  "columns" should "return all columns" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    val df: ChronFrame = ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value

    df.columns shouldBe Seq("name", "age", "height", "city")

    df.close()
  }

}
