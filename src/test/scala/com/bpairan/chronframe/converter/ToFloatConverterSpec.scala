package com.bpairan.chronframe.converter

import cats.implicits._
import com.bpairan.chronframe.converter.ToFloatConverter.convert
import net.openhft.chronicle.bytes.{Bytes, NoBytesStore}
import net.openhft.chronicle.wire.TextWire
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Created by Bharathi Pairan on 26/04/2022.
 */
class ToFloatConverterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  val wire = new TextWire(Bytes.elasticHeapByteBuffer(8))
  val Key = "float"

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

  it should "convert to float" in {
    wire.write(Key).bytes("3.145F".getBytes())
    convert(wire.read(Key)) shouldBe 3.145F.some

    wire.clear()
    wire.write(Key).bytes("3.145".getBytes())
    convert(wire.read(Key)) shouldBe 3.145F.some
  }

  it should "convert double to float" in {
    wire.write(Key).bytes("3.145D".getBytes())
    convert(wire.read(Key)) shouldBe 3.145F.some
  }

  it should "convert int to float" in {
    wire.write(Key).bytes("3".getBytes())
    convert(wire.read(Key)) shouldBe 3.0F.some
  }

  it should "be None for inf" in {
    wire.write(Key).bytes("inf".getBytes())
    convert(wire.read(Key)) shouldBe None
  }
}
