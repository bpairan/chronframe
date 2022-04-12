package com.bpairan.chronframe

import com.bpairan.chronframe.csv.FromCsv
import net.openhft.chronicle.core.OS
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder
import net.openhft.chronicle.queue.{ChronicleQueue, ExcerptTailer}
import net.openhft.chronicle.wire.DocumentContext

import java.nio.file.Path

/**
 * Created by Bharathi Pairan on 04/04/2022.
 */
class DataFrame(queue: ChronicleQueue) extends IndexedSeq[DocumentContext] with AutoCloseable {
  val tailer: ExcerptTailer = queue.createTailer()


  override def apply(i: Int): DocumentContext = {
    tailer.toStart
    tailer.documentAt(i)
  }

  override def length: Int = {
    tailer.toStart
    new DocumentContextIterator(tailer).size
  }

  override def close(): Unit = {
    tailer.close()
    queue.close()
  }
}

object DataFrame {

  def newChronicleQueue(name: String, path: Option[Path] = None): ChronicleQueue = {
    val basePath = path.map(_.toString).getOrElse(s"${OS.getTarget}/$name")
    SingleChronicleQueueBuilder.single(basePath).build()
  }

  def fromCsv(input: Path, output: Option[Path], separator: Char, escape: Char): DataFrame = {
    val queue = newChronicleQueue(input.getFileName.toString, output)
    fromCsv(input, queue, separator, escape)
  }

  def fromCsv(input: Path, queue: ChronicleQueue, separator: Char, escape: Char): DataFrame = {
    new FromCsv(separator, escape).parse(input, queue)
    new DataFrame(queue)
  }
}
