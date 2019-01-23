package org.rzlabs.druid.metadata

case class SpatialDruidDimensionInfo(druidColumn: String,
                                     spatialPosition: Int,
                                     minValue: Option[Double],
                                     maxValue: Option[Double])

case class DruidRelationColumnInfo(column: String,
                                   druidColumn: Option[String],
                                   spatialIndex: Option[SpatialDruidDimensionInfo] = None,
                                   hllMetric: Option[String] = None,
                                   sketchMetric: Option[String] = None,
                                   cardinalityEstimate: Option[Long] = None)
