package com.bpairan.chronframe.converter

import com.bpairan.chronframe.converter.ToStringConverter.convert
import net.openhft.chronicle.bytes.{Bytes, NoBytesStore}
import net.openhft.chronicle.wire.TextWire
import org.scalatest.{BeforeAndAfterEach, OptionValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Created by Bharathi Pairan on 18/06/2022.
 */
class ToStringConverterSpec extends AnyFlatSpec with Matchers with OptionValues with BeforeAndAfterEach {

  val wire = new TextWire(Bytes.elasticHeapByteBuffer(8))
  val Key = "string"

  override def beforeEach(): Unit = {
    wire.clear()
  }

  it should "trim line separators in end of line" in {
    wire.write(Key).bytes("Hello World\n".getBytes())
    convert(wire.read(Key)).value shouldBe "Hello World"
  }

  it should "trim unicode" in {
    wire.write(Key).bytes("Hello\u0001World".getBytes())
    convert(wire.read(Key)).value shouldBe "HelloWorld"
  }

  it should "be None" in {
    wire.write(Key).bytes(NoBytesStore.NO_BYTES)
    convert(wire.read(Key)) shouldBe None
  }
}
