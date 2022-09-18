package com.bpairan.chronframe.converter

import cats.implicits.catsSyntaxOptionId
import com.bpairan.chronframe.converter.ToLongConverter.convert
import net.openhft.chronicle.bytes.{Bytes, NoBytesStore}
import net.openhft.chronicle.wire.TextWire
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Created by Bharathi Pairan on 02/07/2022.
 */
class ToLongConverterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  val wire = new TextWire(Bytes.elasticHeapByteBuffer(8))
  val Key = "long"

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

  it should "be None for null string" in {
    wire.write(Key).bytes("null".getBytes())
    convert(wire.read(Key)) shouldBe None
  }

  it should "be None for un-parsable string" in {
    wire.write(Key).bytes("hello".getBytes())
    convert(wire.read(Key)) shouldBe None
  }

  it should "convert to long" in {
    wire.write(Key).bytes("3145".getBytes())
    convert(wire.read(Key)) shouldBe 3145L.some
  }

  it should "convert to long for values with L suffix" in {
    wire.write(Key).bytes("3145L".getBytes())
    convert(wire.read(Key)) shouldBe 3145L.some
  }
}
