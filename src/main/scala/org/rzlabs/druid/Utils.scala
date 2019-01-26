package org.rzlabs.druid

import org.joda.time.Interval

object Utils {

  def intervalsMillis(intervals: List[Interval]): Long = {
    intervals.foldLeft[Long](0L) {
      case (t, in) => t + (in.getEndMillis - in.getStartMillis)
    }
  }

  def filterSomes[A](a: List[Option[A]]): List[Option[A]] = {
    a.filter { case Some(x) => true; case _ => false }
  }
}
