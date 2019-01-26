package org.rzlabs.druid

import org.joda.time.Interval
import org.rzlabs.druid.metadata.DruidSegmentInfo

case class SegmentInterval(itvl: String,
                           ver: String,
                           part: Option[Int])

case class SegmentIntervals(`type`: String,
                            segments: List[SegmentInterval]) {
  def this(segInAssignments: List[(DruidSegmentInfo, Interval)]) = {
    this("segments", segInAssignments.map {
      case (segInfo, interval) =>
        val itvl: String = interval.toString
        val ver: String = segInfo.version
        val part: Option[Int] = segInfo.shardSpec.flatMap(_.partitionNum)
        SegmentInterval(itvl, ver, part)
    })
  }
}

object SegmentIntervals {

  def segmentIntervals(segInAssignments: List[DruidSegmentInfo]): SegmentIntervals = {
    SegmentIntervals("segments", segInAssignments.map {
      case segInfo =>
        val itvl: String = segInfo.interval
        val ver: String = segInfo.version
        val part: Option[Int] = segInfo.shardSpec.flatMap(_.partitionNum)
        SegmentInterval(itvl, ver, part)
    })
  }
}
