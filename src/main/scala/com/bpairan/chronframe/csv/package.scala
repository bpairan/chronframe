package com.bpairan.chronframe

import cats.data.ValidatedNel

/**
 * Created by Bharathi Pairan on 03/04/2022.
 */
package object csv {
  type CsvParserErrorOr[T] = ValidatedNel[CsvParseError, T]
}
