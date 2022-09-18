package com.bpairan.chronframe.converter

import cats.implicits._
import com.bpairan.chronframe.converter.ToDoubleConverter.convert
import net.openhft.chronicle.bytes.{Bytes, NoBytesStore}
import net.openhft.chronicle.wire.TextWire
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Created by Bharathi Pairan on 18/04/2022.
 */
class ToDoubleConverterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  val wire = new TextWire(Bytes.elasticHeapByteBuffer(8))
  val Key = "double"

  override def beforeEach(): Unit = {
    wire.clear()
  }

  it should "be None for null" in {
    wire.write(Key).bytes(NoBytesStore.NO_BYTES)
    convert(wire.read(Key)) shouldBe None
  }

  it should "be None for NA" in {
    wire.write(Key).bytes("NA".getBytes())
    convert(wire.read(Key)) shouldBe None
  }

  it should "be None for n/a" in {
    wire.write(Key).bytes("n/a".getBytes())
    convert(wire.read(Key)) shouldBe None
  }

  it should "be None for null string" in {
    wire.write(Key).bytes("null".getBytes())
    convert(wire.read(Key)) shouldBe None
  }

  it should "be None for un-parsable string" in {
    wire.write(Key).bytes("hello".getBytes())
    convert(wire.read(Key)) shouldBe None
  }

  it should "convert to double" in {
    wire.write(Key).bytes("3.145D".getBytes())
    convert(wire.read(Key)) shouldBe 3.145D.some

    wire.clear()
    wire.write(Key).bytes("3.145".getBytes())
    convert(wire.read(Key)) shouldBe 3.145D.some
  }

  it should "convert float to double" in {
    wire.write(Key).bytes("3.145F".getBytes())
    convert(wire.read(Key)) shouldBe 3.145D.some
  }

  it should "convert long to double" in {
    wire.write(Key).bytes("3L".getBytes())
    convert(wire.read(Key)) shouldBe 3.0D.some
  }

  it should "be None for nan" in {
    wire.write(Key).bytes("nan".getBytes())
    convert(wire.read(Key)) shouldBe None
  }
}
