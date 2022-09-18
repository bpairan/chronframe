package com.bpairan.chronframe.converter

import cats.implicits._
import net.openhft.chronicle.bytes.{Bytes, NoBytesStore}
import net.openhft.chronicle.wire.TextWire
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Created by Bharathi Pairan on 15/04/2022.
 */
class ToIntConverterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  val wire = new TextWire(Bytes.elasticHeapByteBuffer(8))
  val Key = "Int"

  override def beforeEach(): Unit = {
    wire.clear()
  }

  it should "be None" in {
    wire.write(Key).bytes(NoBytesStore.NO_BYTES)
    ToIntConverter.convert(wire.read(Key)) shouldBe None
  }

  it should "convert comma separated int" in {
    wire.write(Key).bytes("10,000".getBytes)
    ToIntConverter.convert(wire.read(Key)) shouldBe 10000.some
  }

  it should "be None for NA" in {
    wire.write(Key).bytes("NA".getBytes())
    ToIntConverter.convert(wire.read(Key)) shouldBe None
  }

  it should "be None for null string" in {
    wire.write(Key).bytes("null".getBytes())
    ToIntConverter.convert(wire.read(Key)) shouldBe None
  }

  it should "be None for un-parsable string" in {
    wire.write(Key).bytes("hello".getBytes())
    ToIntConverter.convert(wire.read(Key)) shouldBe None
  }
}
