package org.rzlabs.druid

import org.joda.time.Interval

object Utils {

  def intervalsMillis(intervals: List[Interval]): Long = {
    intervals.foldLeft[Long](0L) {
      case (t, in) => t + (in.getEndMillis - in.getStartMillis)
    }
  }
}
