package org.rzlabs.druid

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.spark.sql.MyLogging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}
import org.fasterxml.jackson.databind.ObjectMapper._
import org.rzlabs.druid.metadata._

class DefaultSource extends RelationProvider with MyLogging {

  import DefaultSOurce._

  private def qualifiedName(sqlContext: SQLContext, tableName: String): String = {

    val sessionState = sqlContext.sparkSession.sessionState
    var tableIdentifier = sessionState.sqlParser.parseTableIdentifier(tableName)

    if (!tableIdentifier.database.isDefined) {
      tableIdentifier = tableIdentifier.copy(database = Some(sessionState.catalog.getCurrentDatabase))
    }

    s"${tableIdentifier.database.get}.${tableIdentifier.table}"
  }

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    var sourceDFName = parameters.getOrElse(SOURCE_DF_PARAM,
      throw new DruidDataSourceException(
        s"'$SOURCE_DF_PARAM' must be specified for Druid DataSource"))

    sourceDFName = qualifiedName(sqlContext, sourceDFName)

    val sourceDF = sqlContext.table(sourceDFName)

    val druidDataSource = parameters.getOrElse(DRUID_DS_PARAM,
      throw new DruidDataSourceException(
        s"'$DRUID_DS_PARAM' must be specified for Druid DataSource"))

    val timeDimensionCol = parameters.getOrElse(TIME_DIMENSION_COLUMN_PARAM,
      throw new DruidDataSourceException(
        s"'$TIME_DIMENSION_COLUMN_PARAM' must be specified for Druid DataSource"))

    // validate taime dimension column is in SourceDF
    sourceDF.schema(timeDimensionCol)

    val maxCardinality =
      parameters.getOrElse(MAX_CARDINALITY_PARAM, DEFAULT_MAX_CARDINALITY).toLong
    val cardinalityPerDruidQuery =
      parameters.getOrElse(MAX_CARDINALITY_PER_DRUID_QUERY_PARAM,
        DEFAULT_CARDINALITY_PER_DRUID_QUERY).toLong

    val columnMapping: Map[String, String] =
      parameters.get(SOURCE_TO_DRUID_NAME_MAP_PARAM).map {
        jsonMapper.readValue(_, new TypeReference[Map[String, String]] {})
    }.getOrElse(Map())

    val columnInfos: List[DruidRelationColumnInfo] =
      parameters.get(SOURCE_TO_DRUID_INFO).map {
        jsonMapper.readValue(_, new TypeReference[List[DruidRelationColumnInfo]] {})
      }.getOrElse(List())

    val druidHost = parameters.getOrElse(DRUID_HOST_PARAM, DEFAULT_DRUID_HOST)

    val pushHLLToDruid = parameters.getOrElse(PUSH_HYPERLOGLOG_TO_DRUID,
      DEFAULT_PUSH_HYPERLOGLOG_TO_DRUID).toBoolean

    val streamDruidQueryResults = parameters.getOrElse(STREAM_DRUID_QUERY_RESULTS,
      DEFAULT_STREAM_DRUID_QUERY_RESULTS).toBoolean

    val loadMetadataFromAllSegments = parameters.getOrElse(LOAD_METADATA_FROM_ALL_SEGMENTS,
      DEFAULT_LOAD_METADATA_FROM_ALL_SEGMENTS).toBoolean

    val zkSessionTimeoutMs = parameters.getOrElse(ZK_SESSION_TIMEOUT,
      DEFAULT_ZK_SESSION_TIMEOUT).toInt

    val zkEnableCompression = parameters.getOrElse(ZK_ENABLE_COMPRESSION,
      DEFAULT_ZK_ENABLE_COMPRESSION).toBoolean

    val zkDruidPath = parameters.getOrElse(ZK_DRUID_PATH, DEFAULT_ZK_DRUID_PATH)

    val queryHistorical = parameters.getOrElse(QUERY_HISTORICAL,
      DEFAULT_QUERY_HISTORICAL).toBoolean

    val zkQualifyDiscoveryNames = parameters.getOrElse(ZK_QUALIFY_DISCOVERY_NAMES,
      DEFAULT_ZK_QUALIFY_DISCOVERY_NAMES).toBoolean

    val numSegmentsPerHistoricalQuery = parameters.getOrElse(NUM_SEGMENTS_PER_HISTORICAL_QUERY,
      DEFAULT_NUM_SEGMENTS_PER_HISTORICAL_QUERY).toInt

    val useSmile = parameters.getOrElse(USE_SMILE, DEFAULT_USE_SMILE).toBoolean

    val allowTopN = parameters.getOrElse(ALLOW_TOPN, DEFAULT_ALLOW_TOPN).toBoolean

    val topNMaxThreshold = parameters.getOrElse(TOPN_MAX_THRESHOLD,
      DEFAULT_TOPN_MAX_THRESHOLD).toInt

    val numProcessingThreadsPerHistorical: Option[Int] =
      parameters.get(NUM_PROCESSING_THREADS_PER_HISTORICAL).map(_.toInt)

    val nonAggregateQueryHandling: NonAggregateQueryHandling.Value =
      NonAggregateQueryHandling.withName(
        parameters.getOrElse(NON_AGG_QUERY_HANDLING, DEFAULT_NON_AGG_QUERY_HANDLING))

    val queryGranularity: DruidQueryGranularity =
      DruidQueryGranularity(parameters.getOrElse(QUERY_GRANULARITY, DEFAULT_QUERY_GRANULARITY))

    val options = DruidRelationOptions(
      maxCardinality,
      cardinalityPerDruidQuery,
      pushHLLToDruid,
      streamDruidQueryResults,
      loadMetadataFromAllSegments,
      zkSessionTimeoutMs,
      zkEnableCompression,
      zkDruidPath,
      queryHistorical,
      zkQualifyDiscoveryNames,
      numSegmentsPerHistoricalQuery,
      useSmile,
      nonAggregateQueryHandling,
      queryGranularity,
      allowTopN,
      topNMaxThreshold,
      numProcessingThreadsPerHistorical
    )

    val druidRelation =
  }
}

object DefaultSOurce {

  val SOURCE_DF_PARAM = "sourceDataframe"

  /**
   * DataSource name in Druid.
   */
  val DRUID_DS_PARAM = "druidDataSource"

  val TIME_DIMENSION_COLUMN_PARAM = "timeDimensionColumn"

  /**
   * If the result cardinality of a Query exceeds this value then Query is not
   * converted to a Druid Query.
   */
  val MAX_CARDINALITY_PARAM = "maxResultCardinality"
  val DEFAULT_MAX_CARDINALITY = (1 * 1000 * 1000).toString

  /**
   * If the result size estimante exceeds this number, and attempt is mode to run 'n'
   * druid queries, each of which spans a sub interval of the total time interval.
   * 'n' is computed as `result.size % thisParam + 1`
   */
  val MAX_CARDINALITY_PER_DRUID_QUERY_PARAM = "maxCardinalityPerQuery"
  val DEFAULT_CARDINALITY_PER_DRUID_QUERY = (100 * 1000).toString

  /**
   * Map column names to Druid field names.
   * Specified as a json string.
   */
  val SOURCE_TO_DRUID_NAME_MAP_PARAM = "columnMapping"

  /**
   * List of [[DruidRelationColumnInfo]] that provide
   * details about the source column to Druid linkages.
   *
   */
  val SOURCE_TO_DRUID_INFO = "columnInfos"

  val DRUID_HOST_PARAM = "druidHost"
  val DEFAULT_DRUID_HOST = "localhost"

  // this is only for test purposes
  val DRUID_QUERY = "druidQuery"

  val PUSH_HYPERLOGLOG_TO_DRUID = "pushHLLToDruid"
  val DEFAULT_PUSH_HYPERLOGLOG_TO_DRUID = "true"

  /**
   * Controls whether Query results from Druid are streamed into
   * Spark Operator pipeline. Default is true.
   */
  val STREAM_DRUID_QUERY_RESULTS = "streamDruidQueryResults"
  val DEFAULT_STREAM_DRUID_QUERY_RESULTS = "true"

  /**
   * When loading Druid DataSource metadata should the query interval be
   * the entire dataSource interval, or only the latest segment is enough.
   * Default is to load from all segments; since our query has
   * ("analysisTypes" -> []) the query is cheap.
   */
  val LOAD_METADATA_FROM_ALL_SEGMENTS = "loadMetadataFromAllSegments"
  val DEFAULT_LOAD_METADATA_FROM_ALL_SEGMENTS = "true"

  val ZK_SESSION_TIMEOUT = "zkSessionTimeoutMilliSecs"
  val DEFAULT_ZK_SESSION_TIMEOUT = "30000"

  val ZK_ENABLE_COMPRESSION = "zkEnableCompression"
  val DEFAULT_ZK_ENABLE_COMPRESSION = "true"

  val ZK_DRUID_PATH = "zkDruidPath"
  val DEFAULT_ZK_DRUID_PATH = "/druid"

  val QUERY_HISTORICAL = "queryHistoricalServers"
  val DEFAULT_QUERY_HISTORICAL = "false"

  val ZK_QUALIFY_DISCOVERY_NAMES = "zkQualifyDiscoveryNames"
  val DEFAULT_ZK_QUALIFY_DISCOVERY_NAMES = "true"

  val NUM_SEGMENTS_PER_HISTORICAL_QUERY = "numSegmentsPerHistoricalQuery"
  val DEFAULT_NUM_SEGMENTS_PER_HISTORICAL_QUERY = Int.MaxValue.toString

  val USE_SMILE = "useSmile"
  val DEFAULT_USE_SMILE = "true"

  val NUM_PROCESSING_THREADS_PER_HISTORICAL = "numProcessingThreadsPerHistorical"

  val NON_AGG_QUERY_HANDLING = "nonAggregateQueryHandling"
  val DEFAULT_NON_AGG_QUERY_HANDLING = NonAggregateQueryHandling.PUSH_NONE.toString

  val QUERY_GRANULARITY = "queryGranularity"
  val DEFAULT_QUERY_GRANULARITY = "none"

  val ALLOW_TOPN = "allowTopNRewrite"
  val DEFAULT_ALLOW_TOPN = "false"

  val TOPN_MAX_THRESHOLD = "topNMaxThreshold"
  val DEFAULT_TOPN_MAX_THRESHOLD = 100000.toString
}