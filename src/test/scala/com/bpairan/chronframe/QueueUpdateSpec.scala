package com.bpairan.chronframe

import net.openhft.chronicle.core.OS
import net.openhft.chronicle.core.io.IOTools
import net.openhft.chronicle.queue.{ChronicleQueue, ExcerptAppender, ExcerptTailer}
import net.openhft.chronicle.wire.{DocumentContext, Wire}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Using

/**
 * Created by Bharathi Pairan on 19/04/2022.
 */
class QueueUpdateSpec extends AnyFlatSpec with Matchers {

  ignore should "update data in excerpt" in new TestCase {
    val queue: ChronicleQueue = ChronFrame.newChronicleQueue("update-test")
    val appender: ExcerptAppender = queue.acquireAppender()
    appender.writeDocument(w => w.write("name").bytes("blah".getBytes()).write("age").int32(0))
    private val idx = appender.lastIndexAppended()
    appender.close()

    val tailer: ExcerptTailer = queue.createTailer()
    private val it = new DocumentContextIterator(tailer)
    it.hasNext shouldBe true
    val head: Wire = it.next().wire()
    head.write("age").int32(42)

    val tailer1: ExcerptTailer = queue.createTailer()
    private val it1 = new DocumentContextIterator(tailer1)
    it1.hasNext shouldBe true
    val head1: Wire = it1.next().wire()
    head1.read("name").bytes() shouldBe "blah".getBytes()
    head1.read("age").int32() shouldBe 42
    IOTools.shallowDeleteDirWithFiles(s"${OS.getTarget}/update-test")
  }

}
