package org.rzlabs.druid.client

import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.arrow.vector.types.Types.MinorType
import org.joda.time.{DateTime, Interval}
import org.rzlabs.druid.{DruidQueryGranularity, NoneGranularity}

// All messages are coming from Druid API responses.

@JsonIgnoreProperties(ignoreUnknown = true)
case class SegmentTimeRange(minTime: DateTime, maxTime: DateTime)

@JsonIgnoreProperties(ignoreUnknown = true)
case class CoordDataSourceInfo(segments: SegmentTimeRange)

/**
 * Constructed by the response of `segmentMetadata` query.
 *
 * @param `type` The column data type in Druid.
 * @param size Estimated byte size for the segment columns if they were stored in a flat format.
 * @param cardinality Time or dimension field's cardinality.
 * @param minValue Min value of string type column in segment.
 * @param maxValue Max value of string type column in segment.
 * @param errorMessage Error message of the column.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class ColumnDetails(`type`: String,
                         size: Long,
                        @JsonDeserialize(contentAs = classOf[java.lang.Long])
                         cardinality: Option[Long],
                         minValue: Option[String],
                         maxValue: Option[String],
                         errorMessage: Option[String]) {

  /**
   * Metric and time column have no cardinality.
   */
  def isDimension = cardinality.isDefined
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class Aggregator(`type`: String,
                      name: String,
                      fieldName: String,
                      expression: Option[String])

@JsonIgnoreProperties(ignoreUnknown = true)
case class TimestampSpec(column: String,
                         format: String,
                         missingValue: Option[String])

/**
 * Constructed by the response of `segmentMetadata` query.
 *
 * @param id ${dataSourceName]_${interval}_${injectTime}
 * @param intervals Intervals of segments.
 * @param columns Column map which key is the column name in Druid.
 * @param size The estimated byte size for the dataSource.
 * @param numRows Total row number of the dataSource.
 * @param aggregators Aggregators specification.
 * @param timestampSpec Timestamp specification.
 * @param queryGranularity Query granularity specified in the ingestion spec.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class MetadataResponse(id: String,
                            intervals: List[String],
                            columns: Map[String, ColumnDetails],
                            size: Long,
                           @JsonDeserialize(contentAs = classOf[java.lang.Long])
                            numRows: Option[Long],
                            aggregators: Option[Map[String, Aggregator]] = None,
                            timestampSpec: Option[TimestampSpec] = None,
                            queryGranularity: Option[DruidQueryGranularity] = None) {

  def getIntervals: List[Interval] = intervals.map(Interval.parse(_))

  /**
   * All intervals' total time tick number.
   * According to different query granularities,
   * same intervals may have different time ticks.
   *
   * @param ins The input interval list.
   * @return The time tick number.
   */
  def timeTicks(ins: List[Interval]): Long =
    queryGranularity.getOrElse(NoneGranularity()).ndv(ins)

  /**
   * Non String dimension columns have no cardinality,
   * we call `getOrElse(1)` just in case.
   */
  def getNumRows: Long = numRows.getOrElse {
    val p = columns.values.filter(c => c.isDimension)
      .map(_.cardinality.getOrElse(1L)).map(_.toDouble).product
    if (p > Long.MaxValue) Long.MaxValue else p.toLong
  }
}
