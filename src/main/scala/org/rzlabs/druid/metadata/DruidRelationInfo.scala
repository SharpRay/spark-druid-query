package org.rzlabs.druid.metadata

import org.apache.spark.sql.types._
import org.apache.spark.sql.{MyLogging, SQLContext}
import org.rzlabs.druid.{DruidQueryGranularity, RectangularBound}

import scala.collection.mutable.{ArrayBuffer, Map => MMap}

object NonAggregateQueryHandling extends Enumeration {
  val PUSH_FILTERS = Value("push_filters")
  val PUSH_PROJECT_AND_FILTERS = Value("push_project_and_filters")
  val PUSH_NONE = Value("push_none")
}

case class DruidRelationName(sparkDataSource: String,
                             druidHost: String,
                             druidDataSource: String)

case class DruidRelationInfo(val fullName: DruidRelationName,
                             val sourceDFName: String,
                             val timeDimensionCol: String,
                             val sourceToDruidMapping: Map[String, DruidRelationColumn],
                             val options: DruidRelationOptions) {

  val host: String = fullName.druidHost

  lazy val dimensionNamesSet = druidDataSource.dimensions.map(_.name).toSet

  def sourceDF(sqlContext: SQLContext) = sqlContext.table(sourceDFName)

  def druidDataSource: DruidDataSource = DruidMetadataCache.getDataSourceInfo(fullName, options)._2

  override def toString: String = {
    s"""DruidRelationInfo(fullName = $fullName, sourceDFName = $sourceDFName,
       |timeDimensionCol = $timeDimensionCol,
       |options = $options)""".stripMargin
  }

  lazy val spatialIndexMap: Map[String, SpatialIndex] = {

    val m: MMap[String, ArrayBuffer[DruidRelationColumn]] = MMap()

    val spatialColumns = sourceToDruidMapping.values.filter(_.hasSpatialIndex)

    spatialColumns.foreach { c =>
      val sI = c.spatialIndex.get
      if (!m.contains(sI.druidColumn.name)) {
        m(sI.druidColumn.name) = ArrayBuffer()
      }
      m(sI.druidColumn.name) += c
    }

    m.map {
      case (k, v) => (k -> SpatialIndex(k, v.toArray))
    }.toMap
  }
}

case class SpatialIndex(name: String, dimColumns: Array[DruidRelationColumn]) {

  private lazy val minArray: Array[Double] =
    dimColumns.map(_.spatialIndex.get.minValue.getOrElse(-Double.MaxValue))
  private lazy val maxArray: Array[Double] =
    dimColumns.map(_.spatialIndex.get.maxValue.getOrElse(Double.MaxValue))

  def getBounds(dim: Int,
                value: Double,
                isMin: Boolean,
                includeVal: Boolean): RectangularBound = {
    val boundValue = {
      if (!includeVal) {
        if (isMin) {
          value + Double.MinPositiveValue
        } else {
          value - Double.MinPositiveValue
        }
      } else {
        value
      }
    }

    val minBounds = minArray.clone()
    val maxBounds = maxArray.clone()
    val boundToChange = if (isMin) minBounds else maxBounds
    boundToChange(dim) = boundValue
    RectangularBound(minBounds, maxBounds)
  }
}

case class DruidRelationOptions(val maxCardinality: Long,
                                val cardinalityPerDruidQuery: Long,
                                pushHLLToDruid: Boolean,
                                streamDruidQueryResults: Boolean,
                                loadMetadataFromAllSegments: Boolean,
                                zkSessionTimeoutMs: Int,
                                zkEnableCompression: Boolean,
                                zkDruidPath: String,
                                queryHistoricalServers: Boolean,
                                zkQualifyDiscoveryNames: Boolean,
                                numSegmentsPerHistoricalQuery: Int,
                                useSmile: Boolean,
                                nonAggQueryHandling: NonAggregateQueryHandling.Value,
                                queryGranularity: DruidQueryGranularity,
                                allowTopN: Boolean,
                                topNMaxThreshold: Int,
                                numProcessingThreadsPerHistorical: Option[Int] = None) {

  def sqlContextOption(nm: String) = s"spark.rzlabs.druid.option.$nm"

  def numSegmentsPerHistoricalQuery(sqlContext: SQLContext): Int = {
    sqlContext.getConf(
      sqlContextOption("numSegmentsPerHistoricalQuery"),
      numSegmentsPerHistoricalQuery.toString
    ).toInt
  }

  def queryHistoricalServers(sqlContext: SQLContext): Boolean = {
    sqlContext.getConf(
      sqlContextOption("queryHistoricalServers"),
      queryHistoricalServers.toString
    ).toBoolean
  }

  def useSmile(sqlContext: SQLContext): Boolean = {
    sqlContext.getConf(
      sqlContextOption("useSmile"),
      useSmile.toString
    ).toBoolean
  }

  def allowTopN(sqlContext: SQLContext): Boolean = {
    sqlContext.getConf(
      sqlContextOption("allowTopN"),
      allowTopN.toString
    ).toBoolean
  }

  def topNMaxThreshold(sqlContext: SQLContext): Int = {
    sqlContext.getConf(
      sqlContextOption("topNMaxThreshold"),
      topNMaxThreshold.toString
    ).toInt
  }
}

private[druid] object MappingBuilder extends MyLogging {

  /**
   * Only top level Numeric and String Types are mapped.
   * @param dT
   * @return
   */
  def supportedDataType(dT: DataType): Boolean = dT match {
    case t if t.isInstanceOf[NumericType] => true
    case StringType => true
    case DateType => true
    case TimestampType => true
    case BooleanType => true
    case _ => false
  }

  private def fillInColumnInfos(sqlContext: SQLContext,
                                sourceDFName: String,
                                nameMapping: Map[String, String],
                                columnInfos: List[DruidRelationColumnInfo],
                                timeDimensionCol: String,
                                druidDS: DruidDataSource): Map[String, DruidRelationColumnInfo] = {

    val userProvidedColInfo: Map[String, DruidRelationColumnInfo] =
      columnInfos.map(info => info.column -> info).toMap

    val m = MMap[String, DruidRelationColumnInfo]()

    val df = sqlContext.table(sourceDFName)
    df.schema.iterator.foreach { field =>
      val cInfo = userProvidedColInfo.getOrElse(field.name,
        DruidRelationColumnInfo(field.name,
          Some(nameMapping.getOrElse(field.name, field.name))
        )
      )
      m += (field.name -> cInfo)
    }

    m.toMap
  }

  def buildMapping(sqlContext: SQLContext,
                   sourceDFName: String,
                   nameMapping: Map[String, String],
                   columnInfos: List[DruidRelationColumnInfo],
                   timeDimensionCol: String,
                   druidDS: DruidDataSource): Map[String, DruidRelationColumn] = {

    val m = MMap[String, DruidRelationColumn]()
    val colInfoMap: Map[String, DruidRelationColumnInfo] = fillInColumnInfos(sqlContext, sourceDFName,
      nameMapping, columnInfos, timeDimensionCol, druidDS)

    val df = sqlContext.table(sourceDFName)
    df.schema.iterator.foreach { field =>
      if (supportedDataType(field.dataType)) {
        val colInfo: DruidRelationColumnInfo = colInfoMap(field.name)
        val relationColumn: Option[DruidRelationColumn] =
          DruidRelationColumn(druidDS, timeDimensionCol, colInfo)
        if (relationColumn.isDefined) {
          m += (field.name -> relationColumn.get)
        } else {
          logWarning(s"${field.name} not mapped to Druid datasource, illegal ColumnInfo $colInfo")
        }
      } else {
        logWarning(s"${field.name} not mapped to Druid dataSource, unsupported dataType")
      }
    }

    m.toMap
  }
}