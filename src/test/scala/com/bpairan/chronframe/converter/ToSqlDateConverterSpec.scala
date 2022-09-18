package com.bpairan.chronframe.converter

import cats.implicits._
import net.openhft.chronicle.bytes.{Bytes, NoBytesStore}
import net.openhft.chronicle.wire.TextWire
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

/**
 * Created by Bharathi Pairan on 14/07/2022.
 */
class ToSqlDateConverterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  val wire = new TextWire(Bytes.elasticHeapByteBuffer(8))
  val Key = "sqlDate"

  override def beforeEach(): Unit = {
    wire.clear()
  }

  it should "be None for null" in {
    wire.write(Key).bytes(NoBytesStore.NO_BYTES)
    ToSqlDateConverter.convert(wire.read(Key)) shouldBe None
  }

  it should "be None for NA" in {
    wire.write(Key).bytes("NA".getBytes())
    ToSqlDateConverter.convert(wire.read(Key)) shouldBe None
  }

  it should "be None for n/a" in {
    wire.write(Key).bytes("n/a".getBytes())
    ToSqlDateConverter.convert(wire.read(Key)) shouldBe None
  }

  it should "be None for null string" in {
    wire.write(Key).bytes("null".getBytes())
    ToSqlDateConverter.convert(wire.read(Key)) shouldBe None
  }

  it should "be None for unknown format" in {
    wire.write(Key).bytes("blah-bl-ah".getBytes())
    ToSqlDateConverter.convert(wire.read(Key)) shouldBe None
  }

  it should "convert yyyyMMdd" in {
    wire.write(Key).bytes("20030201".getBytes())
    ToSqlDateConverter.convert(wire.read(Key)) shouldBe java.sql.Date.valueOf(LocalDate.of(2003, 2, 1)).some
  }

  it should "convert yyyy/MM/dd" in {
    wire.write(Key).bytes("2003/02/01".getBytes())
    ToSqlDateConverter.convert(wire.read(Key)) shouldBe java.sql.Date.valueOf(LocalDate.of(2003, 2, 1)).some
  }

  it should "convert yyyy-MM-dd" in {
    wire.write(Key).bytes("2003-02-01".getBytes())
    ToSqlDateConverter.convert(wire.read(Key)) shouldBe java.sql.Date.valueOf(LocalDate.of(2003, 2, 1)).some
  }

  it should "convert MM/dd/yy" in {
    wire.write(Key).bytes("02/01/03".getBytes())
    ToSqlDateConverter.convert(wire.read(Key)) shouldBe java.sql.Date.valueOf(LocalDate.of(2003, 2, 1)).some
  }

  it should "convert MM-dd-yy" in {
    wire.write(Key).bytes("02-01-03".getBytes())
    ToSqlDateConverter.convert(wire.read(Key)) shouldBe java.sql.Date.valueOf(LocalDate.of(2003, 2, 1)).some
  }

}
