package org.apache.spark.sql.sources.druid

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SQLConf._
import org.rzlabs.druid._


class DruidPlanner {

}

object DruidPlanner {

  val RZLABS_CACHE_TABLE_TOCHECK = buildConf(
    "spark.rzlabs.druid.cache.tables.tocheck")
    .doc("A comma separated list of tableNames that should be checked if they are cached. " +
      "For Start-Schemas with associated Druid Indexes, even if tables are cached, we " +
      "attempt to rewrite the Query to Druid. In order to do this we need to convert an " +
      "[[InMemoryRelation]] operator with its underlying logical Plan. This value, will " +
      "tell us to restrict our check to certain tables. Otherwise by default we will check " +
      "all tables.").stringConf.toSequence.createWithDefault(List())

  val DEBUG_TRANSFORMATIONS = buildConf("spark.rzlabs.druid.debug.transformations")
    .doc("When set to true each transformation is logged.")
    .booleanConf.createWithDefault(false)

  val TZ_ID = buildConf("spark.rzlabs.tz.id")
    .doc("specify the TimeZone ID of the spark: used by Druid for" +
      "Date/Timestamp transformations.").stringConf
    .createWithDefault(org.joda.time.DateTimeZone.getDefault.getID)

  val DRUID_SELECT_QUERY_PAGESIZE = buildConf("spark.rzlabs.druid.selectquery.pagesize")
    .doc("Num. of rows fetched on each invocation of Druid Select Query.")
    .intConf.createWithDefault(10000)

  val DRUID_CONN_POOL_MAX_CONNECTIONS = buildConf("spark.rzlabs.druid.max.connections")
    .doc("Max. number of Http connections to Druid Cluster")
    .intConf.createWithDefault(100)

  val DRUID_CONN_POOL_MAX_CONNECTIONS_PER_ROUTE = buildConf(
    "spark.rzlabs.druid.max.connections.per.route")
    .doc("Max. number of Http connections to each server in Druid Cluster.")
    .intConf.createWithDefault(20)

  val DRUID_QUERY_COST_MODEL_ENABLED = buildConf(
    "spark.rzlabs.druid.querycostmodel,enabled")
    .doc("flag that controls if decision to execute druid query on broker or " +
      "historicals is based on the cost model; default is true. If false the " +
      "decision is based on the 'queryHistoricalServers' and 'numSegmentsPerHistoricalQuery' " +
      "datasource options.").booleanConf.createWithDefault(true)

  val DRUID_QUERY_COST_MODEL_HIST_MERGE_COST = buildConf(
    "spark.rzlabs.druid.querycostmodel.histMergeCostFactor")
    .doc("cost of performing a segment agg. merge in druid " +
      "historicals relative to spark shuffle cost.").doubleConf.createWithDefault(0.07)

  val DRUID_QUERY_COST_MODEL_HIST_SEGMENTS_PERQUERY_LIMIT = buildConf(
    "spark.rzlabs.druid.querycostmodel.histSegsPerQueryLimit")
    .doc("the max. number of segments processed per historical query.")
    .intConf.createWithDefault(5)

  val DRUID_QUERY_COST_MODEL_INTERVAL_SCALING_NDV = buildConf(
    "spark.rzlabs.druid.querycostmodel.queryintervalScalingForDistinctValues")
    .doc("The ndv estimate for a query interval uses this number. The ratio of " +
      "the (query interval/index interval) is multiplied by this number. " +
      "The ndv for a query is estimated as: " +
      "'log(this_value * min(10 * interval_ratio, 10)) * orig_ndv'. The reduction is logarithmic " +
      "and this value applies further dampening factor on the reduction. At a default value " +
      "of '3' any interval ratio >= 0.33 will have no reduction in ndvs.")
    .doubleConf.createWithDefault(3)

  val DRUID_QUERY_COST_MODEL_HISTORICAL_PROCESSING_COST = buildConf(
    "spark.rzlabs.druid.querycostmodel.historicalProcessingCost")
    .doc("the cost per row of groupBy processing in historical servers " +
      "relative to spark shuffle cost").doubleConf.createWithDefault(0.1)

  val DRUID_QUERY_COST_MODEL_HISTORICAL_TIMESERIES_PROCESSING_COST = buildConf(
    "spark.rzlabs.druid.querycostmodel.historicalTimeSeriesProcessingCost")
    .doc("the cost per row of timeseries processing in historical servers " +
      "relative to spark shuffle cost").doubleConf.createWithDefault(0.07)

  val DRUID_QUERY_COST_MODEL_SPARK_SCHEDULING_COST = buildConf(
    "spark.rzlabs.druid.querycostmodel.sparkSchedulingCost")
    .doc("the cost of scheduling tasks in spark relative the the shuffle cost of 1 row")
    .doubleConf.createWithDefault(1.0)

  val DRUID_QUERY_COST_MODEL_SPARK_AGGREGATING_COST = buildConf(
    "spark.rzlabs.druid.querycostmodel.sparkAggregatingCost")
    .doc("the cost per row to do aggregation in spark relative to the shuffle cost")
    .doubleConf.createWithDefault(0.15)

  val DRUID_QUERY_COST_MODEL_OUTPUT_TRANSPORT_COST = buildConf(
    "spark.rzlabs.druid.querycostmodel.druidOutputTransportCost")
    .doc("the cost per row to transport druid output relative to the shuffle cost.")
    .doubleConf.createWithDefault(0.4)

  val DRUID_USE_SMILE = buildConf("spark.rzlabs.druid.option.useSmile")
    .doc("for druid queries use the smile binary protocol")
    .booleanConf.createWithDefault(true)

  val DRUID_ALLOW_TOPN_QUERIES = buildConf("spark.rzlabs.druid.allowTopN")
    .doc("druid TopN queries are approximate in their aggregation and ranking,  this " +
      "flag controls if TopN query rewrites should happen.")
    .booleanConf.createWithDefault(DefaultSOurce.DEFAULT_ALLOW_TOPN.toBoolean)

  val DRUID_TOPN_QUERIES_MAX_THRESHOLD = buildConf("spark.rzlbas.druid.topNMaxThreshold")
    .doc("if druid TopN queries are enabled, this property controls the maximum " +
      "limit for which such rewrites are done. For limits beyond this value the GroupBy query is " +
      "executed.").intConf.createWithDefault(DefaultSOurce.DEFAULT_TOPN_MAX_THRESHOLD.toInt)

  val DRUID_USE_V2_GBY_ENGINE = buildConf("spark.rzlabs.druid.option.use.v2.groupByEngine")
    .doc("for druid queries use the v2 groupBy engine.")
    .booleanConf.createWithDefault(false)

  val DRUID_RECORD_QUERY_EXECUTION = buildConf("spark.rzlabs.druid.enable.query.history")
    .doc("track each druid query executed from every Spark Executor.")
    .booleanConf.createWithDefault(false)

  def getConfValue[T](sqlContext: SQLContext, entry: ConfigEntry[T]): T = {
    sqlContext.conf.getConf(entry)
  }
}
