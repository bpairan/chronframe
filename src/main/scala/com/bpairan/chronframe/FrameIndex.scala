package com.bpairan.chronframe

/**
 * Holds the frame index and the current list of columns
 * Outer list size represents the number of writes/mutations, if the DataFrame is created during initialisation then only one item will be there in outer list
 *
 * Created by Bharathi Pairan on 02/06/2022.
 */
case class FrameIndex(idx: List[List[Long]], columns: List[Seq[String]])
