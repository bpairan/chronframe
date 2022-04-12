package com.bpairan.chronframe

import com.bpairan.chronframe
import net.openhft.chronicle.wire.{DocumentContext, Wire}

/**
 * Created by Bharathi Pairan on 04/04/2022.
 */
class DataFrameRow(wire: Wire) {

}

object DataFrameRow {
  implicit class DocumentContextToDataFrameRowConverter(val dc: DocumentContext) extends AnyVal {
    def toDataFrameRow: DataFrameRow = {
      new chronframe.DataFrameRow(dc.wire())
    }
  }
}
