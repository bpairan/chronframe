package com.bpairan.chronframe

import cats.data.ValidatedNel

/**
 * Created by Bharathi Pairan on 14/07/2022.
 */
package object parser {
  type ParserErrorOr[T] = ValidatedNel[ParseError, T]
}
