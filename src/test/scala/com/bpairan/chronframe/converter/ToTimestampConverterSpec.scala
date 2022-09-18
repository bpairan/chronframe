package com.bpairan.chronframe.converter

import cats.implicits._
import net.openhft.chronicle.bytes.{Bytes, NoBytesStore}
import net.openhft.chronicle.wire.TextWire
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDateTime

/**
 * Created by Bharathi Pairan on 15/07/2022.
 */
class ToTimestampConverterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  val wire = new TextWire(Bytes.elasticHeapByteBuffer(8))
  val Key = "localDateTime"
  private val ExpectedTime = Timestamp.valueOf(LocalDateTime.of(2003, 2, 1, 14, 5, 2)).some

  override def beforeEach(): Unit = {
    wire.clear()
  }

  it should "be None for null" in {
    wire.write(Key).bytes(NoBytesStore.NO_BYTES)
    ToTimestampConverter.convert(wire.read(Key)) shouldBe None
  }

  it should "be None for NA" in {
    wire.write(Key).bytes("NA".getBytes())
    ToTimestampConverter.convert(wire.read(Key)) shouldBe None
  }

  it should "be None for n/a" in {
    wire.write(Key).bytes("n/a".getBytes())
    ToTimestampConverter.convert(wire.read(Key)) shouldBe None
  }

  it should "be None for null string" in {
    wire.write(Key).bytes("null".getBytes())
    ToTimestampConverter.convert(wire.read(Key)) shouldBe None
  }

  it should "be None for unknown format" in {
    wire.write(Key).bytes("blah-bl-ah".getBytes())
    ToTimestampConverter.convert(wire.read(Key)) shouldBe None
  }

  it should "convert yyyy/MM/dd HH:mm:ss format" in {
    wire.write(Key).bytes("2003/02/01 14:05:02".getBytes())
    ToTimestampConverter.convert(wire.read(Key)) shouldBe ExpectedTime
  }

  it should "convert yyyy-MM-dd HH:mm:ss format" in {
    wire.write(Key).bytes("2003-02-01 14:05:02".getBytes())
    ToTimestampConverter.convert(wire.read(Key)) shouldBe ExpectedTime
  }

  it should "convert MM-dd-yyyy HH:mm:ss format" in {
    wire.write(Key).bytes("02-01-2003 14:05:02".getBytes())
    ToTimestampConverter.convert(wire.read(Key)) shouldBe ExpectedTime
  }

  it should "convert MM/dd/yyyy HH:mm:ss format" in {
    wire.write(Key).bytes("02/01/2003 14:05:02".getBytes())
    ToTimestampConverter.convert(wire.read(Key)) shouldBe ExpectedTime
  }

  it should "convert ISO Date Time format" in {
    wire.write(Key).bytes("2003-02-01T14:05:02+01:00".getBytes())
    ToTimestampConverter.convert(wire.read(Key)) shouldBe ExpectedTime
  }


  it should "convert ISO Local Date Time format" in {
    wire.write(Key).bytes("2003-02-01T14:05:02".getBytes())
    ToTimestampConverter.convert(wire.read(Key)) shouldBe ExpectedTime
  }

}
