package com.bpairan.chronframe.parser

import net.openhft.chronicle.queue.ChronicleQueue
import org.mockito.Mockito.{mock, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import cats.implicits._
/**
 * Created by Bharathi Pairan on 15/07/2022.
 */
class WriteDataSpec extends AnyFlatSpec with Matchers {

  "Invalid header" should "be error" in {
    val parser = mock(classOf[ChronParser])
    val error = LineParseError(1, "error").invalidNel
    when(parser.header()).thenReturn(error)
    WriteData.from(parser, mock(classOf[ChronicleQueue])) shouldBe error
  }

}
