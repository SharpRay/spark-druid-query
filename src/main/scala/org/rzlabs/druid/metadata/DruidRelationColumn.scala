package org.rzlabs.druid.metadata

import org.rzlabs.druid.Utils

case class SpatialDruidDimensionInfo(druidColumn: String,
                                     spatialPosition: Int,
                                     minValue: Option[Double],
                                     maxValue: Option[Double])

case class SpatialDruidDimension(druidColumn: DruidDimension,
                                 spatialPosition: Int,
                                 minValue: Option[Double],
                                 maxValue: Option[Double])

case class DruidRelationColumnInfo(column: String,
                                   druidColumn: Option[String],
                                   spatialIndex: Option[SpatialDruidDimensionInfo] = None,
                                   hllMetric: Option[String] = None,
                                   sketchMetric: Option[String] = None,
                                   cardinalityEstimate: Option[Long] = None)

case class DruidRelationColumn(column: String,
                               druidColumn: Option[DruidColumn],
                               spatialIndex: Option[SpatialDruidDimension] = None,
                               hllMetric: Option[DruidMetric] = None,
                               sketchMetric: Option[DruidMetric] = None,
                               cardinalityEstimate: Option[Long] = None) {

  private lazy val druidColumnToUse: DruidColumn = {
    Utils.filterSomes(
      List(druidColumn, hllMetric, sketchMetric, spatialIndex.map(_.druidColumn))
    ).head.get
  }

  def hasDirectDruidColumn = druidColumn.isDefined
  def hasSpatialIndex = spatialIndex.isDefined
  def hasHLLMetric = hllMetric.isDefined
  def hasSketchMetric = sketchMetric.isDefined

  def name = druidColumnToUse.name

  def dataType = if (hasSpatialIndex) DruidDataType.Float else druidColumnToUse.dataType

  def size = druidColumnToUse.size

  val cardinality: Long = cardinalityEstimate.getOrElse(druidColumnToUse.cardinality)

  def isDimension(excludeTime: Boolean = false): Boolean = {
    hasDirectDruidColumn && druidColumnToUse.isDimension(excludeTime)
  }

  def isTimeDimension: Boolean = {
    hasDirectDruidColumn && druidColumnToUse.isInstanceOf[DruidTimeDimension]
  }

  def isMetric: Boolean = hasDirectDruidColumn && !isDimension(false)

  def metric: DruidMetric = druidColumnToUse.asInstanceOf[DruidMetric]
}

object DruidRelationColumn {

  def apply(druidDataSource: DruidDataSource,
            timeDimensionCol: String,
            colInfo: DruidRelationColumnInfo): Option[DruidRelationColumn] = {

    (colInfo.druidColumn, colInfo.spatialIndex, colInfo.hllMetric, colInfo.sketchMetric) match {
      case (Some(dC), None, None, None) if (dC == timeDimensionCol) =>
        val druidColumn: DruidColumn = druidDataSource.timeDimension.get
        Some(
          new DruidRelationColumn(colInfo.column,
            Some(druidColumn),
            None, None, None, colInfo.cardinalityEstimate)
        )
      case (Some(dC), None, None, None) if druidDataSource.columns.contains(dC) =>
        val druidColumn: DruidColumn = druidDataSource.columns(dC)
        Some(
          new DruidRelationColumn(colInfo.column,
            Some(druidColumn),
            None, None, None, colInfo.cardinalityEstimate)
        )
      case (odC, Some(sI), None, None)
        if druidDataSource.columns.contains(sI.druidColumn) &&
          druidDataSource.columns(sI.druidColumn).isDimension() => {

        val druidRelationColumn = if (odC.isDefined) {
          apply(druidDataSource, timeDimensionCol, colInfo.copy(spatialIndex = None))
        } else None

        if (odC.isDefined && !druidRelationColumn.isDefined) {
          return None
        }

        Some(
          new DruidRelationColumn(colInfo.column,
            druidRelationColumn.flatMap(_.druidColumn),
            Some(
              SpatialDruidDimension(druidDataSource.columns(sI.druidColumn).asInstanceOf[DruidDimension],
                sI.spatialPosition, sI.minValue, sI.maxValue)
            ),
            None, None, colInfo.cardinalityEstimate)
        )
      }
      case (odC, None, hllMetric, sketchMetric)
        if hllMetric.isDefined || sketchMetric.isDefined => {

        val drC = if (odC.isDefined) {
          apply(druidDataSource, timeDimensionCol, colInfo.copy(hllMetric = None, sketchMetric = None))
        } else None

        if (odC.isDefined && !drC.isDefined) {
          return None
        }

        var hllM: Option[DruidMetric] = None
        var sketchM: Option[DruidMetric] = None

        if (hllMetric.isDefined) {
          if (!druidDataSource.columns.contains(hllMetric.get) ||
            druidDataSource.columns(hllMetric.get).isDimension()) {
            return None
          }
          hllM = Some(druidDataSource.columns(hllMetric.get).asInstanceOf[DruidMetric])
        }

        if (sketchMetric.isDefined) {
          if (!druidDataSource.columns.contains(sketchMetric.get) ||
            druidDataSource.columns(sketchMetric.get).isDimension()) {
            return None
          }
          sketchM = Some(druidDataSource.columns(sketchMetric.get).asInstanceOf[DruidMetric])
        }

        if (hllM.isDefined || sketchM.isDefined) {
          Some(new DruidRelationColumn(colInfo.column,
            drC.flatMap(_.druidColumn),
            None, hllM, sketchM, colInfo.cardinalityEstimate))
        } else None
      }
      case _ => None
    }
  }

  def apply(dC: DruidColumn): DruidRelationColumn = {
    new DruidRelationColumn(dC.name, Some(dC), None, None, None, None)
  }
}