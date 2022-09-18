package com.bpairan.chronframe.converter

import com.bpairan.chronframe.converter.ToBooleanConverter.convert
import net.openhft.chronicle.bytes.{Bytes, NoBytesStore}
import net.openhft.chronicle.wire.TextWire
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Created by Bharathi Pairan on 18/04/2022.
 */
class ToBooleanConverterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  val wire = new TextWire(Bytes.elasticHeapByteBuffer(8))
  val Key = "Flag"

  override def beforeEach(): Unit = {
    wire.clear()
  }

  it should "be None" in {
    wire.write(Key).bytes(NoBytesStore.NO_BYTES)
    convert(wire.read(Key)) shouldBe None
  }

  it should "be None for NA" in {
    wire.write(Key).bytes("NA".getBytes)
    convert(wire.read(Key)) shouldBe None
  }

  it should "be None for n/a" in {
    wire.write(Key).bytes("n/a".getBytes)
    convert(wire.read(Key)) shouldBe None
  }

  it should "be None for null string" in {
    wire.write(Key).bytes("null".getBytes)
    convert(wire.read(Key)) shouldBe None
  }

  it should "be true" in {
    wire.write(Key).bytes("true".getBytes())
    convert(wire.read(Key)) shouldBe Some(true)
  }

  it should "be false" in {
    wire.write(Key).bytes("false".getBytes())
    convert(wire.read(Key)) shouldBe Some(false)
  }

  it should "convert Y to true" in {
    wire.write(Key).bytes("Y".getBytes())
    convert(wire.read(Key)) shouldBe Some(true)
  }

  it should "convert N to false" in {
    wire.write(Key).bytes("N".getBytes())
    convert(wire.read(Key)) shouldBe Some(false)
  }

  it should "convert 1 to true" in {
    wire.write(Key).bytes("1".getBytes())
    convert(wire.read(Key)) shouldBe Some(true)

    wire.clear()
    wire.write(Key).bytes(Array('1'.toByte))
    convert(wire.read(Key)) shouldBe Some(true)
  }

  it should "convert 0 to false" in {
    wire.write(Key).bytes("0".getBytes())
    convert(wire.read(Key)) shouldBe Some(false)

    wire.clear()
    wire.write(Key).bytes(Array('0'.toByte))
    convert(wire.read(Key)) shouldBe Some(false)
  }

  it should "convert yes to true" in {
    wire.write(Key).bytes("yes".getBytes())
    convert(wire.read(Key)) shouldBe Some(true)
  }

  it should "convert no to true" in {
    wire.write(Key).bytes("no".getBytes())
    convert(wire.read(Key)) shouldBe Some(false)
  }

}
