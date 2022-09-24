package com.bpairan.chronframe

import cats.implicits._
import com.bpairan.chronframe.test.TestUtils
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Using

/**
 * Created by Bharathi Pairan on 04/04/2022.
 */
class ChronFrameSpec extends AnyFlatSpec with Matchers with TestUtils with OptionValues {

  it should "return length" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    Using.resource(ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value) { cf =>
      cf.length shouldBe 2
    }
  }

  it should "get first document" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    Using.resource(ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value) { cf =>
      // head
      val row0 = cf(0)
      val name0 = row0.valueOf[String]("name")
      name0 shouldBe "Emily".some
      row0.valueOf[Int]("age") shouldBe 33.some
      row0.valueOf[Int]("height") shouldBe 169.some
      row0.valueOf[String]("city") shouldBe "London".some
    }
  }

  it should "read null" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    Using.resource(ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value) { cf =>
      val row = cf(1)
      row.valueOf[String]("name") shouldBe "Thomas".some
      row.valueOf[Int]("age") shouldBe 25.some
      row.valueOf[Int]("height") shouldBe None
      row.valueOf[String]("city") shouldBe None
    }
  }

  it should "throw exception" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    Using.resource(ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value) { cf =>
      the[IllegalArgumentException] thrownBy cf(2) should have message "index: 2 not found"
    }
  }

  "rename" should "rename existing column" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    Using.resource(ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value) { cf =>
      val cfRenamed = cf.rename("city", "place")

      val row: ChronFrameRow = cfRenamed(0)
      row.valueOf[String]("place") shouldBe "London".some
    }
  }

  it should "rename multiple columns" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    Using.resource(ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value) { cf =>
      val cfRenamed = cf.rename(Map("height" -> "height_cm", "city" -> "place"))

      val row: ChronFrameRow = cfRenamed(0)
      row.valueOf[Int]("height_cm") shouldBe 169.some
      row.valueOf[String]("place") shouldBe "London".some
    }
  }

  "drop" should "drop columns from frame" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    Using.resource(ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value) { cf =>

      val cfDropped = cf.drop("height", "city")

      val row1: ChronFrameRow = cfDropped(0)
      row1.valueOf[String]("name") shouldBe "Emily".some
      row1.valueOf[Int]("age") shouldBe 33.some
      row1.valueOf[String]("height") shouldBe None
      row1.valueOf[String]("city") shouldBe None

      val row2: ChronFrameRow = cfDropped(1)
      row2.valueOf[String]("name") shouldBe "Thomas".some
      row2.valueOf[Int]("age") shouldBe 25.some
      row2.valueOf[String]("height") shouldBe None
      row2.valueOf[String]("city") shouldBe None
    }
  }

  "addRow" should "add a new row" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    Using.resource(ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value) { cf =>

      val cfAdded = cf.addRow(Map("name" -> "X", "age" -> 43, "height" -> 180, "city" -> "Yard"))

      val row1: ChronFrameRow = cfAdded(0)
      row1.valueOf[String]("name") shouldBe "Emily".some
      row1.valueOf[Int]("age") shouldBe 33.some
      row1.valueOf[Int]("height") shouldBe 169.some
      row1.valueOf[String]("city") shouldBe "London".some

      val row2: ChronFrameRow = cfAdded(1)
      row2.valueOf[String]("name") shouldBe "Thomas".some
      row2.valueOf[Int]("age") shouldBe 25.some
      row2.valueOf[String]("height") shouldBe None
      row2.valueOf[String]("city") shouldBe None

      val row3: ChronFrameRow = cfAdded(2)
      row3.valueOf[String]("name") shouldBe "X".some
      row3.valueOf[Int]("age") shouldBe 43.some
      row3.valueOf[Int]("height") shouldBe 180.some
      row3.valueOf[String]("city") shouldBe "Yard".some
    }
  }

  "addRows" should "add all rows" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    Using.resource(ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value) { cf =>

      val data = List(
        Map("name" -> "X", "age" -> 43, "height" -> 180, "city" -> "Yard"),
        Map("name" -> "Y", "age" -> 13, "height" -> 160, "city" -> "Boat")
      )

      val cfAdded = cf.addRows(data)

      val row1: ChronFrameRow = cfAdded(0)
      row1.valueOf[String]("name") shouldBe "Emily".some
      row1.valueOf[Int]("age") shouldBe 33.some
      row1.valueOf[Int]("height") shouldBe 169.some
      row1.valueOf[String]("city") shouldBe "London".some

      val row2: ChronFrameRow = cfAdded(1)
      row2.valueOf[String]("name") shouldBe "Thomas".some
      row2.valueOf[Int]("age") shouldBe 25.some
      row2.valueOf[String]("height") shouldBe None
      row2.valueOf[String]("city") shouldBe None

      val row3: ChronFrameRow = cfAdded(2)
      row3.valueOf[String]("name") shouldBe "X".some
      row3.valueOf[Int]("age") shouldBe 43.some
      row3.valueOf[Int]("height") shouldBe 180.some
      row3.valueOf[String]("city") shouldBe "Yard".some

      val row4: ChronFrameRow = cfAdded(3)
      row4[String]("name") shouldBe "Y".some
      row4[Int]("age") shouldBe 13.some
      row4[Int]("height") shouldBe 160.some
      row4[String]("city") shouldBe "Boat".some
    }
  }

  "appendColumn" should "throw exception when size of data is not same as size of ChronFrame" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    Using.resource(ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value) { cf =>
      val exception: IllegalArgumentException = intercept[IllegalArgumentException] {
        cf.appendColumn("country", Seq("UK"))
      }
      exception.getMessage shouldBe "size of data should be same as size of ChronFrame"
    }
  }

  it should "append new column" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    Using.resource(ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.map(_.appendColumn("country", Seq("UK", "France"))).value) { cf =>

      val row1: ChronFrameRow = cf(0)
      row1.valueOf[String]("name") shouldBe "Emily".some
      row1.valueOf[Int]("age") shouldBe 33.some
      row1.valueOf[Int]("height") shouldBe 169.some
      row1.valueOf[String]("city") shouldBe "London".some
      row1.valueOf[String]("country") shouldBe "UK".some

      val row2: ChronFrameRow = cf(1)
      row2.valueOf[String]("name") shouldBe "Thomas".some
      row2.valueOf[Int]("age") shouldBe 25.some
      row2.valueOf[String]("height") shouldBe None
      row2.valueOf[String]("city") shouldBe None
      row2.valueOf[String]("country") shouldBe "France".some
    }
  }

  "columns" should "return all columns" in new TestCase {
    private val inputPath = testResource("sample.csv")
    private val queue = ChronFrame.newChronicleQueue("test")
    Using.resource(ChronFrame.fromCsv(inputPath, queue, ',', '"').toOption.value) { cf =>
      cf.columns shouldBe Seq("name", "age", "height", "city")
    }
  }

}
