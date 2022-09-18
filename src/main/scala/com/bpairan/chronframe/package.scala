package com.bpairan

import net.openhft.chronicle.queue.ExcerptTailer
import net.openhft.chronicle.wire.DocumentContext

/**
 * Created by Bharathi Pairan on 04/04/2022.
 */
package object chronframe {
  class DocumentContextIterator(tailer: ExcerptTailer) extends Iterator[DocumentContext] {
    private var dc: DocumentContext = _

    override def hasNext: Boolean = {
      if (dc != null) dc.close()
      dc = tailer.readingDocument()
      dc.isPresent
    }

    override def next(): DocumentContext = {
      dc
    }
  }

}
