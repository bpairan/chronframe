package com.bpairan.chronframe.converter

/**
 * Created by Bharathi Pairan on 13/04/2022.
 */
object Ordering {
  object CaseInsensitive extends Ordering[String] {
    override def compare(x: String, y: String): Int = x.compareToIgnoreCase(y)
  }

}
