package org.rzlabs.druid.client

import java.io.IOException
import java.util.concurrent.ExecutorService

import org.apache.curator.framework.api.CompressionProvider
import org.apache.curator.framework.imps.GzipCompressionProvider
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.cache.{ChildData, PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.curator.utils.{CloseableUtils, ZKPaths}
import org.apache.spark.sql.MyLogging
import org.apache.spark.util.MyThreadUtils
import org.rzlabs.druid.metadata.{DruidMetadataCache, DruidNode, DruidRelationOptions}
import org.fasterxml.jackson.databind.ObjectMapper._
import org.rzlabs.druid.DruidDataSourceException

import scala.collection.mutable.{Map => MMap}
import scala.util.Try

class CuratorConnection(val zkHost: String,
                        val options: DruidRelationOptions,
                        val cache: DruidMetadataCache,
                        execSvc: ExecutorService) extends MyLogging {

  val serverSegmentsCache: MMap[String, PathChildrenCache] = MMap()
  val serverSegmentCacheLock = new Object

  // In /druid/discovery, the subdir druid:broker, druid:overlord, druid:coordinator exists
  val discoveryPath = ZKPaths.makePath(options.zkDruidPath, "discovery")
  // In /druid/announcements, the historical servers exists, e.g., /druid/announcements/spark2:8083
  val announcementPath = ZKPaths.makePath(options.zkDruidPath, "announcements")
  // In /druid/segments, the historical servers exists, e.g., /druid/segments/spark2:8083
  val serverSegmentsPath = ZKPaths.makePath(options.zkDruidPath, "segments")

  val framework: CuratorFramework = CuratorFrameworkFactory.builder.connectString(zkHost)
    .sessionTimeoutMs(options.zkSessionTimeoutMs)
    .retryPolicy(new BoundedExponentialBackoffRetry(1000, 45000, 30))
    .compressionProvider(new PotentiallyGzippedCompressionProvider(options.zkEnableCompression))
    .build()

  val announcementsCache: PathChildrenCache = new PathChildrenCache(
    framework,
    announcementPath,
    true,
    true,
    execSvc
  )

  val serverSegmentsPathCache: PathChildrenCache = new PathChildrenCache(
    framework,
    serverSegmentsPath,
    true,
    true,
    execSvc
  )

  val listener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED |
          PathChildrenCacheEvent.Type.CHILD_REMOVED => {
          logWarning(s"Event received: ${event.getType}, path: ${event.getData.getPath}")
          cache.clearCache(zkHost)
        }
        case _ => ()
      }
    }
  }

  val serverSegmentListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED => { // add historical server in path /druid/segments
          serverSegmentCacheLock.synchronized {
            val child: ChildData = event.getData
            val key = getServerKey(event) // /druid/segment/spark1:8083, spark1:8083 is the key
            if (serverSegmentsCache.contains(key)) {
              log.error(
                "New node[%s] but there was already one in serverSegmentsCache. " +
                  "That's not good, ignoring new one.", child.getPath)
            } else {
              val segmentsPath: String = String.format("%s/%s", serverSegmentsPath, key)
              val segmentsCache: PathChildrenCache = new PathChildrenCache(
                framework,
                segmentsPath,
                true,
                true,
                execSvc
              )
              segmentsCache.getListenable.addListener(listener) // listen change in /druid/segments/spark1:8083
              serverSegmentsCache(key) = segmentsCache
              logDebug(s"Starting inventory cache for $key, inventoryPath $segmentsPath")
              segmentsCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT)
            }
          }
        }
        case PathChildrenCacheEvent.Type.CHILD_REMOVED => {
          serverSegmentCacheLock.synchronized {
            val child: ChildData = event.getData
            val key = getServerKey(event)
            val segmentsCache: Option[PathChildrenCache] = serverSegmentsCache.remove(key)
            if (segmentsCache.isDefined) {
              log.debug("Closing inventory cache for %s. Also removing listeners.", key)
              segmentsCache.get.getListenable.clear()
              segmentsCache.get.close()
            } else {
              log.warn("Container[%s] removed that wasn't a container!?", child.getPath)
            }
          }
        }
        case _ => ()
      }
    }
  }

  // listen path /druid/announcements for historical servers
  announcementsCache.getListenable.addListener(listener)
  // listen path /druid/segmeents for historical servers
  serverSegmentsPathCache.getListenable.addListener(serverSegmentListener)

  framework.start()
  announcementsCache.start(StartMode.BUILD_INITIAL_CACHE)
  serverSegmentsPathCache.start(StartMode.POST_INITIALIZED_EVENT)

  // trigger loading class CloseableUtils
  CloseableUtils.closeQuietly(null)

  MyThreadUtils.addShutdownHook { () =>
    Try { announcementsCache.close() }
    Try { serverSegmentsPathCache.close() }
    Try {
      serverSegmentCacheLock.synchronized {
        serverSegmentsCache.values.foreach { inventoryCache =>
          inventoryCache.getListenable.clear()
          inventoryCache.close()
        }
      }
    }
    Try { CloseableUtils.closeQuietly(framework) }
  }

  def getService(name: String): String = {
    val discoveryName = if (options.zkQualifyDiscoveryNames) s"${options.zkDruidPath}:$name" else name

    val fullDiscoveryPath = ZKPaths.makePath(discoveryPath, discoveryName)
    // may have more than one broker, but just one active coordinator and overlord.
    val serviceNames: java.util.List[String] = framework.getChildren.forPath(fullDiscoveryPath)

    try {
      val serviceName = serviceNames.get(scala.util.Random.nextInt(serviceNames.size()))
      val servicePath = ZKPaths.makePath(fullDiscoveryPath, serviceName)
      val data: Array[Byte] = framework.getData.forPath(servicePath)
      val serviceDetail = jsonMapper.readValue(new String(data), classOf[DruidNode])
      s"${serviceDetail.address}:${serviceDetail.port}"
    } catch {
      case e: Exception =>
        throw new DruidDataSourceException(s"Failed to get '$name' for '$zkHost'", e)
    }
  }

  def getBroker: String = {
    getService("broker")
  }

  def getCoordinator: String = {
    getService("coordinator")
  }


  private def getServerKey(event: PathChildrenCacheEvent): String = {
    val child: ChildData = event.getData

    val data: Array[Byte] = getZkDataForNode(child.getPath)
    if (data == null) {
      log.info("Ignoring event: Type - %s, Path - %s, Version - %s",
        Array(event.getType, child.getPath, child.getStat.getVersion))
      null
    } else {
      ZKPaths.getNodeFromPath(child.getPath)
    }
  }

  private def getZkDataForNode(path: String): Array[Byte] = {
    try {
      framework.getData.decompressed().forPath(path)
    } catch {
      case ex: Exception => {
        log.warn(s"Exception while getting data for node $path", ex)
        null
      }
    }
  }

}

/*
 * copied from druid code base.
 */
class PotentiallyGzippedCompressionProvider(val compressOutput: Boolean)
  extends CompressionProvider {

  private val base: GzipCompressionProvider = new GzipCompressionProvider


  @throws[Exception]
  def compress(path: String, data: Array[Byte]): Array[Byte] = {
    return if (compressOutput) base.compress(path, data)
    else data
  }

  @throws[Exception]
  def decompress(path: String, data: Array[Byte]): Array[Byte] = {
    try {
      return base.decompress(path, data)
    }
    catch {
      case e: IOException => {
        return data
      }
    }
  }
}
