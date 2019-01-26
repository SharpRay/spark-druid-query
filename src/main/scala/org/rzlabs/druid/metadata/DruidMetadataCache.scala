package org.rzlabs.druid.metadata

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.apache.spark.sql.{DataFrame, MyLogging, SQLContext}
import org.apache.spark.util.MyThreadUtils
import org.joda.time.Interval
import org.rzlabs.druid.client.{CuratorConnection, DruidCoordinatorClient, DruidQueryServerClient}

import scala.collection.mutable.{Map => MMap}
import scala.concurrent.ExecutionContext

case class ModuleInfo(name: String,
                      artifact: String,
                      version: String)

case class ServerMemory(maxMemory: Long,
                        totalMemory: Long,
                        freeMemory: Long,
                        usedMemory: Long)

case class ServerStatus(version: String,
                        modules: List[ModuleInfo],
                        memory: ServerMemory)

case class DruidNode(name: String,
                     id: String,
                     address: String,
                     port: Int)

case class ShardSpec(`type`: String,
                     partitionNum: Option[Int],
                     Partitions: Option[Int])

@JsonIgnoreProperties(ignoreUnknown = true)
case class DruidSegmentInfo(dataSource: String,
                            interval: String,
                            version: String,
                            binaryVersion: String,
                            size: Long,
                            identifier: String,
                            shardSpec: Option[ShardSpec]) {

  lazy val _interval: Interval = Interval.parse(interval)

  def overlaps(in: Interval): Boolean = _interval.overlaps(in)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class DataSourceSegmentInfo(name: String,
                                 properties: Map[String, String],
                                 segments: List[DruidSegmentInfo]) {

  def segmentsToScan(in: Interval): List[DruidSegmentInfo] = segments.filter(_.overlaps(in))
}

case class HistoricalServerAssignment(server: HistoricalServerInfo,
                                      segmentIntervals: List[(DruidSegmentInfo, Interval)])

@JsonIgnoreProperties(ignoreUnknown = true)
case class HistoricalServerInfo(host: String,
                                maxSize: Long,
                               `type`: String,
                                tier: String,
                                priority: Int,
                                segments: Map[String, DruidSegmentInfo], // segmentId -> segmentInfo
                                currSize: Long) {

  def handlesSegment(segId: String) = segments.contains(segId)
}

case class DruidClusterInfo(host: String,
                            curatorConnection: CuratorConnection,
                            coordinatorStatus: ServerStatus,
                            druidDataSources: MMap[String, (DataSourceSegmentInfo, DruidDataSource)],  // dataSource ->
                            histServers: List[HistoricalServerInfo]) {

  def historicalServers(druidRelationName: DruidRelationName,
                        ins: List[Interval]): List[HistoricalServerAssignment] = {

    // historical server host ->
    val m: MMap[String, (HistoricalServerInfo, List[(DruidSegmentInfo, Interval)])] = MMap()

    /**
     * - favor the higher priority server
     * - when the priorities are equal
     * - favor the server with least work.
     */
    implicit val o = new Ordering[HistoricalServerInfo] {
      override def compare(x: HistoricalServerInfo, y: HistoricalServerInfo): Int = {
        if (x.priority == y.priority) {
          (m.get(x.host), m.get(y.host)) match {
            case (None, None) => 0 // x
            case (None, _) => -1 // x
            case (_, None) => 1 // y
            case (Some((i1, l1)), Some((i2, l2))) => l1.size - l2.size
          }
        } else {
          y.priority - x.priority
        }
      }
    }

    for (in <- ins) {
      druidDataSources(druidRelationName.druidDataSource)._1.segmentsToScan(in).foreach { seg =>
        val overlappedSegIn = seg._interval.overlap(in)
        val chosenHistServer = histServers.filter(_.handlesSegment(seg.identifier)).sorted.head
        if (m.contains(chosenHistServer.host)) {
          val v = m(chosenHistServer.host) // (HistoricalServerInfo, List[(DruidSegmentInfo, Interval)])
          val segInTuple = (seg, overlappedSegIn)
          m(chosenHistServer.host) = (v._1, segInTuple :: v._2)
        } else {
          m(chosenHistServer.host) = (chosenHistServer, List((seg, overlappedSegIn)))
        }
      }
    }

    m.map {
      case (h, t) => HistoricalServerAssignment(t._1, t._2)
    }.toList
  }
}

trait DruidMetadataCache {
  type DruidDataSourceInfo = (DataSourceSegmentInfo, DruidDataSource)

  def getDruidClusterInfo(druidRelationName: DruidRelationName,
                          options: DruidRelationOptions): DruidClusterInfo

  def getDataSourceInfo(druidRelationName: DruidRelationName,
                        options: DruidRelationOptions): DruidDataSourceInfo

  def assignHistoricalServers(druidRelationName: DruidRelationName,
                              options: DruidRelationOptions,
                              intervals: List[Interval]): List[HistoricalServerAssignment]

  def register(druidRelationName: DruidRelationName,
               options: DruidRelationOptions): Unit

  def clearCache(host: String): Unit
}

trait DruidRelationInfoCache {

  self: DruidMetadataCache =>

  def druidRelation(sqlContext: SQLContext,
                    sourceDFName: String,
                    sourceDF: DataFrame,
                    dsName: String,
                    timeDimensionCol: String,
                    druidHost: String,
                    columnMapping: Map[String, String],
                    columnInfos: List[DruidRelationColumnInfo],
                    options: DruidRelationOptions): DruidRelationInfo = {

    val fullName = DruidRelationName(sourceDFName, druidHost, dsName)

    val druidDataSource: DruidDataSource = getDataSourceInfo(fullName, options)._2

    val sourceToDruidMapping =
      MappingBuilder.buildMapping(sqlContext, sourceDFName, columnMapping,
        columnInfos, timeDimensionCol, druidDataSource)

    val druidRelationInfo = DruidRelationInfo(fullName, sourceDFName,
      timeDimensionCol, sourceToDruidMapping, options)

    druidRelationInfo
  }
}

object DruidMetadataCache extends DruidMetadataCache with DruidRelationInfoCache with MyLogging {

  private[metadata] val cache: MMap[String, DruidClusterInfo] = MMap()
  private val curatorConnections: MMap[String, CuratorConnection] = MMap()
  val threadPool = MyThreadUtils.newDaemonCachedThreadPool("druidZkEventExec", 10)
  implicit val ec = ExecutionContext.fromExecutor(threadPool)

  private def curatorConnection(host: String,
                                options: DruidRelationOptions): CuratorConnection = {
    curatorConnections.getOrElse(host, {
      val cc = new CuratorConnection(host, options, this, threadPool)
      curatorConnections(host) = cc
      cc
    })
  }

  def getDruidClusterInfo(druidRelationName: DruidRelationName,
                          options: DruidRelationOptions): DruidClusterInfo = {
    cache.synchronized {
      if (cache.contains(druidRelationName.druidHost)) {
        cache(druidRelationName.druidHost)
      } else {
        val zkHost = druidRelationName.druidHost
        val cc = curatorConnection(zkHost, options)
        val coordinator = cc.getCoordinator
        val coordClient = new DruidCoordinatorClient(coordinator)
        val serverStatus = coordClient.serverStatus
        val historicalServersInfo: List[HistoricalServerInfo] =
          coordClient.serversInfo.filter(_.`type` == "historical")
        val druidClusterInfo = new DruidClusterInfo(zkHost, cc, serverStatus,
          MMap[String, DruidDataSourceInfo](), historicalServersInfo)
        cache(druidClusterInfo.host) = druidClusterInfo
        log.info(s"loading druid cluster info for ${druidRelationName}")
        druidClusterInfo
      }
    }
  }

  def getDataSourceInfo(druidRelationName: DruidRelationName,
                        options: DruidRelationOptions): DruidDataSourceInfo = {
    val druidClusterInfo = getDruidClusterInfo(druidRelationName, options)
    druidClusterInfo.synchronized {
      if (druidClusterInfo.druidDataSources.contains(druidRelationName.druidDataSource)) {
        druidClusterInfo.druidDataSources(druidRelationName.druidDataSource)
      } else {
        val coordClient =
          new DruidCoordinatorClient(druidClusterInfo.curatorConnection.getCoordinator)
        val dataSourceInfo: DataSourceSegmentInfo =
          coordClient.dataSourceInfo(druidRelationName.druidDataSource)
        val brokerClient =
          new DruidQueryServerClient(druidClusterInfo.curatorConnection.getBroker, false)
        var druidDataSource: DruidDataSource = brokerClient.metadata(druidRelationName.druidDataSource,
          options.loadMetadataFromAllSegments, druidClusterInfo.coordinatorStatus.version)
        druidDataSource = druidDataSource.copy(druidVersion = druidClusterInfo.coordinatorStatus.version)
        val druidDataSourceInfo: DruidDataSourceInfo = (dataSourceInfo, druidDataSource)
        val m = druidClusterInfo.druidDataSources
        m(druidDataSourceInfo._1.name) = druidDataSourceInfo
        log.info(s"loading druid datasource info for ${druidRelationName}")
        druidDataSourceInfo

      }
    }
  }

  def assignHistoricalServers(druidRelationName: DruidRelationName,
                              options: DruidRelationOptions,
                              intervals: List[Interval]): List[HistoricalServerAssignment] = {
    val druidClusterInfo = cache.synchronized {
      getDataSourceInfo(druidRelationName, options)
      getDruidClusterInfo(druidRelationName, options)
    }
    druidClusterInfo.historicalServers(druidRelationName, intervals)
  }

  def register(druidRelationName: DruidRelationName, options: DruidRelationOptions) = {
    getDataSourceInfo(druidRelationName, options)
  }

  def clearCache: Unit = cache.synchronized(cache.clear())

  def clearCache(host: String): Unit = cache.synchronized {
    log.warn(s"clearing cache for ${host} at \n${(new Throwable).getStackTrace}")
    cache.remove(host)
  }
}