package com.bpairan

import net.openhft.chronicle.queue.ExcerptTailer
import net.openhft.chronicle.wire.DocumentContext

import scala.util.control.Breaks.{break, breakable}

/**
 * Created by Bharathi Pairan on 04/04/2022.
 */
package object chronframe {
  class DocumentContextIterator(tailer: ExcerptTailer) extends Iterator[DocumentContext] {
    private var dc: DocumentContext = _

    override def hasNext: Boolean = {
      println("hasNext")
      if (dc != null) dc.close()
      dc = tailer.readingDocument()
      dc.isPresent
    }

    override def next(): DocumentContext = {
      println("next")
      dc
    }
  }


  implicit class TailerIndexAt(val tailer: ExcerptTailer) extends AnyVal {
    def documentAt(idx: Int): DocumentContext = {
      println(s"Requested index:$idx")

      var i = 0
      val it = new DocumentContextIterator(tailer)
      var dc: Option[DocumentContext] = None
      breakable {
        while (it.hasNext) {
          if (i == idx) {
            dc = Option(it.next())
            break
          }
          if (i > idx) break else i += 1
        }
      }
      dc.getOrElse(throw new IllegalArgumentException(s"index: $i not found"))
    }
  }
}
