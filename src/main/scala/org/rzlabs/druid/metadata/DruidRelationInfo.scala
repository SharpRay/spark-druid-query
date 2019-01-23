package org.rzlabs.druid.metadata

import org.apache.spark.sql.SQLContext
import org.rzlabs.druid.DruidQueryGranularity

object NonAggregateQueryHandling extends Enumeration {
  val PUSH_FILTERS = Value("push_filters")
  val PUSH_PROJECT_AND_FILTERS = Value("push_project_and_filters")
  val PUSH_NONE = Value("push_none")
}

case class DruidRelationName(sparkDataSource: String,
                             druidHost: String,
                             druidDataSource: String)

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
