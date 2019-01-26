package org.rzlabs.druid.client

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.http.HttpEntity
import org.apache.http.HttpHeaders
import org.apache.http.client.methods._
import org.apache.http.concurrent.Cancellable
import org.apache.http.entity.{ByteArrayEntity, ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.{MyLogging, SQLContext}
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.rzlabs.druid.{DruidDataSourceException, DruidQueryGranularity, SegmentIntervals}
import org.fasterxml.jackson.databind.ObjectMapper._
import org.joda.time.{DateTime, Interval}
import org.rzlabs.druid.metadata._

import scala.util.Try

object ConnectionManager {

  @volatile private var initialized: Boolean = false

  lazy val pool = {
    val p = new PoolingHttpClientConnectionManager
    p.setMaxTotal(40)
    p.setDefaultMaxPerRoute(8)
    p
  }

  def init(sqlContext: SQLContext): Unit = {
    if (!initialized) {
      init(DruidPlanner.getConfValue(sqlContext,
        DruidPlanner.DRUID_CONN_POOL_MAX_CONNECTIONS_PER_ROUTE),
        DruidPlanner.getConfValue(sqlContext,
          DruidPlanner.DRUID_CONN_POOL_MAX_CONNECTIONS))
      initialized = true
    }
  }

  def init(maxPerRoute: Int, maxTotal: Int): Unit = {
    if (!initialized) {
      pool.setMaxTotal(maxTotal)
      pool.setDefaultMaxPerRoute(maxPerRoute)
      initialized = true
    }
  }
}

/**
 * A mechanism the relay [[org.apache.http.concurrent.Cancellable]] resources
 * associated with the ```http connection``` of a ``DruidClient``. This is
 * used by the [[org.rzlabs.druid.TaskCancelHandler]] to capture the
 * association between ```Spark Tasks``` and ``Cancellable`` resources.
 */
trait CancellableHolder {
  def setCancellable(c: Cancellable)
}

/**
 * A mixin trait that relays [[org.apache.http.concurrent.Cancellable]] resources
 * to a [[org.rzlabs.druid.client.CancellableHolder]]
 */
trait DruidClientHttpExecutionAware extends HttpExecutionAware {

  val ch: CancellableHolder

  abstract override def isAborted = super.isAborted

  abstract override def setCancellable(cancellable: Cancellable): Unit = {
    if (ch != null) {
      ch.setCancellable(cancellable)
    }
    super.setCancellable(cancellable)
  }
}

/**
 * Configure HttpPost to have the [[org.rzlabs.druid.client.DruidClientHttpExecutionAware]]
 * trait, so that [[org.apache.http.concurrent.Cancellable]] resources are relayed to the
 * registered holder.
 */
class DruidHttpPost(url: String,
                    val ch: CancellableHolder)
  extends HttpPost(url) with DruidClientHttpExecutionAware

/**
 * Configure HttpGet to have the [[org.rzlabs.druid.client.DruidClientHttpExecutionAware]]
 * trait, so that [[org.apache.http.concurrent.Cancellable]] resources are relayed to the
 * registered holder.
 */
class DruidHttpGet(url: String,
                   val ch: CancellableHolder)
  extends HttpGet(url) with DruidClientHttpExecutionAware

/**
 * ''DruidClient''  is not thread-safe. ''cancellableHolder'' state is used to relay
 * cancellable resources information.
 */
abstract class DruidClient(val host: String,
                           val port: Int,
                           val useSmile: Boolean = false) extends MyLogging {

  private var cancellableHolder: CancellableHolder = null

  def this(t: (String, Int)) = {
    this(t._1, t._2)
  }

  def this(s: String) = {
    this(DruidClient.hostPort(s))
  }

  def setCancellableHolder(c: CancellableHolder): Unit = {
    cancellableHolder = c
  }

  /**
   * A [[CloseableHttpClient]] is a [[org.apache.http.client.HttpClient]]
   * with a `close` method in [[java.io.Closeable]]
   * @return
   */
  protected def httpClient: CloseableHttpClient = {
    val sTime = System.currentTimeMillis()
    val r = HttpClients.custom().setConnectionManager(ConnectionManager.pool).build()
    val eTime = System.currentTimeMillis()
    log.debug("Time to get httpClient: {}", eTime - sTime)
    log.debug("Pool Stats: {}", ConnectionManager.pool.getTotalStats)
    r
  }

  /**
   * Close the [[java.io.InputStream]] represented by the
   * `resp.getEntity.getContent()` to return a
   * [[org.apache.http.client.HttpClient]] to the
   * connection pool.
   * @param resp
   */
  protected def release(resp: CloseableHttpResponse): Unit = {
    Try {
      if (resp != null) EntityUtils.consume(resp.getEntity)
    } recover {
      case ex => log.error("Error returning client to pool", ExceptionUtils.getStackTrace(ex))
    }
  }

  protected def getRequest(url: String) = new DruidHttpGet(url, cancellableHolder)
  protected def postRequest(url: String) = new DruidHttpPost(url, cancellableHolder)

  protected def addHeaders(req: HttpRequestBase, reqHeaders: Map[String, String]): Unit = {
    if (useSmile) {
      req.addHeader(HttpHeaders.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
    }
    if (reqHeaders != null) {
      reqHeaders.foreach(header => req.setHeader(header._1, header._2))
    }
  }

  @throws[DruidDataSourceException]
  protected def perform(url: String,
                        reqType: String => HttpRequestBase,
                        payload: ObjectNode,
                        reqHeaders: Map[String, String]): String = {
    var resp: CloseableHttpResponse = null

    val tis: Try[String] = for {
      r <- Try {
        val req: CloseableHttpClient = httpClient
        val request = reqType(url)
        // Just HttpPost extends HttpEntityEnclosingRequestBase.
        // HttpGet extends HttpRequestBase.
        if (payload != null && request.isInstanceOf[HttpEntityEnclosingRequestBase]) {
          val input: HttpEntity = if (!useSmile) {
            new StringEntity(jsonMapper.writeValueAsString(payload), ContentType.APPLICATION_JSON)
          } else {
            new ByteArrayEntity(smileMapper.writeValueAsBytes(payload), null)
          }
          request.asInstanceOf[HttpEntityEnclosingRequestBase].setEntity(input)
        }
        addHeaders(request, reqHeaders)
        resp = req.execute(request)
        resp
      }
      is <- Try {
        val status = r.getStatusLine.getStatusCode
        if (status >= 200 && status < 300) {
          if (r.getEntity != null) {
            IOUtils.toString(r.getEntity.getContent)
          } else {
            throw new DruidDataSourceException(s"Unexpected response status: ${r.getStatusLine}")
          }
        } else {
          throw new DruidDataSourceException(s"Unexpected response status: ${r.getStatusLine}")
        }
      }
    } yield is

    release(resp)
    tis.getOrElse(tis.failed.get match {
      case de: DruidDataSourceException => throw de
      case e => throw new DruidDataSourceException("Failed in communication with Druid", e)
    })
  }

//  protected def performQuery(url: String,
//                             reqType: String => HttpRequestBase,
//                             qrySpec: QuerySpec,
//                             payload: ObjectNode,
//                             reqHeaders: Map[String, String]): CloseableIterator[ResultRow]

  protected def post(url: String,
                     payload: ObjectNode,
                     reqHeaders: Map[String, String] = null): String = {
    perform(url, postRequest _, payload, reqHeaders)
  }

  protected def get(url: String,
                    payload: ObjectNode = null,
                    reqHeaders: Map[String, String] = null): String = {
    perform(url, getRequest _, payload, reqHeaders)
  }

//  @throws[DruidDataSourceException]
//  def executeQuery(url)

//  def executeQueryAsStream

  def timeBoundary(dataSource: String): Interval

  @throws[DruidDataSourceException]
  def metadata(url: String,
               dataSource: String,
               segs: List[DruidSegmentInfo],
               fullIndex: Boolean,
               druidVersion: String): DruidDataSource = {

    val in: Interval = timeBoundary(dataSource)
    val ins: JsonNode = if (segs != null) {
      jsonMapper.readTree(jsonMapper.writeValueAsString(SegmentIntervals.segmentIntervals(segs)))
    } else {
      val i = if (fullIndex) in.toString else in.withEnd(in.getStart.plusMillis(1)).toString
      jsonMapper.readTree(jsonMapper.writeValueAsString(List(i)))
    }

    val payload: ObjectNode = if (!DruidDataSourceCapability.supportsQueryGranularityMetadata(druidVersion)) {
      jsonMapper.createObjectNode()
        .put("queryType", "segmentMetadata")
        .put("dataSource", dataSource)
        .set("intervals", ins).asInstanceOf[ObjectNode]
        .set("analysisTypes", jsonMapper.createArrayNode()
          .add("cardinality")
          .add("interval")
          .add("aggregators")).asInstanceOf[ObjectNode]
        .put("merge", true)
    } else if (!DruidDataSourceCapability.supportsTimestampSpecMetadata(druidVersion)) {
      jsonMapper.createObjectNode()
        .put("queryType", "segmentMetadata")
        .put("dataSource", dataSource)
        .set("intervals", ins).asInstanceOf[ObjectNode]
        .set("analysisTypes", jsonMapper.createArrayNode()
          .add("cardinality")
          .add("interval")
          .add("aggregators")
          .add("queryGranularity")).asInstanceOf[ObjectNode]
        .put("merge", true)
    } else {
      jsonMapper.createObjectNode()
        .put("queryType", "segmentMetadata")
        .put("dataSource", dataSource)
        .set("intervals", ins).asInstanceOf[ObjectNode]
        .set("analysisTypes", jsonMapper.createArrayNode()
          .add("cardinality")
          .add("interval")
          .add("aggregators")
          .add("queryGranularity")
          .add("timestampSpec")).asInstanceOf[ObjectNode]
        .put("merge", true)
    }

    val resp: String = post(url, payload)
    logWarning(s"The json response of 'segmentMetadata query: \n$resp'")

    // substitute  `queryGranularity` field value if needed.
    val resp1 = jsonMapper.writeValueAsString(DruidQueryGranularity.substitute(
      jsonMapper.readTree(resp).path(0)))
    logWarning(s"After substitution, the json: \n$resp1")

    val mr: MetadataResponse =
      jsonMapper.readValue(resp1, new TypeReference[MetadataResponse] {})
    DruidDataSource(dataSource, mr, List(in))
  }

  def serverStatus: ServerStatus = {
    val url = s"http://$host:$port/status"
    val is: String = get(url)
    jsonMapper.readValue(is, new TypeReference[ServerStatus] {})
  }
}

object DruidClient {

  val HOST = """([^:]*):(\d*)""".r

  def hostPort(s: String): (String, Int) = {
    val HOST(h, p) = s
    (h, p.toInt)
  }
}

class DruidQueryServerClient(host: String, port: Int, useSmile: Boolean = false)
  extends DruidClient(host, port, useSmile) {

  @transient val url = s"http://$host:$port/druid/v2/?pretty"

  def this(t: (String, Int), useSmile: Boolean) = {
    this(t._1, t._2, useSmile)
  }

  def this(s: String, useSmile: Boolean) = {
    this(DruidClient.hostPort(s), useSmile)
  }

  @throws[DruidDataSourceException]
  override def timeBoundary(dataSource: String): Interval = {
    val payload: ObjectNode = jsonMapper.createObjectNode()
      .put("queryType", "timeBoundary")
      .put("dataSource", dataSource)
    val resp: String = post(url, payload)
    val objectNode = jsonMapper.readTree(resp)
    val maxTime: java.util.List[String] = objectNode.findValuesAsText("maxTime")
    val minTime: java.util.List[String] = objectNode.findValuesAsText("minTime")
    if (!maxTime.isEmpty && !minTime.isEmpty) {
      new Interval(
        DateTime.parse(minTime.get(0)),
        DateTime.parse(maxTime.get(0)).plusMillis(1)
      )
    } else {
      throw new DruidDataSourceException("Time boundary should include both the start time and the end time.")
    }
  }

  @throws[DruidDataSourceException]
  def metadata(dataSource: String, fullIndex: Boolean, druidVersion: String): DruidDataSource = {
    metadata(url, dataSource, null, fullIndex, druidVersion)
  }
}

class DruidCoordinatorClient(host: String, port: Int, useSmile: Boolean = false)
  extends DruidClient(host, port, useSmile) {

  @transient val urlPrefix = s"http://$host:$port/druid/coordinator/v1"

  def this(t: (String, Int)) = {
    this(t._1, t._2)
  }

  def this(s: String) = {
    this(DruidClient.hostPort(s))
  }

  @throws[DruidDataSourceException]
  override def timeBoundary(dataSource: String): Interval = {
    val url = s"$urlPrefix/datasources/$dataSource"
    val resp: String = get(url)
    val coordDataSourceInfo: CoordDataSourceInfo =
      jsonMapper.readValue(resp, new TypeReference[CoordDataSourceInfo] {})
    new Interval(coordDataSourceInfo.segments.minTime,
      coordDataSourceInfo.segments.maxTime)
  }

  @throws[DruidDataSourceException]
  def serversInfo: List[HistoricalServerInfo] = {
    val url = s"$urlPrefix/servers?full=true"
    val resp: String = get(url)
    jsonMapper.readValue(resp, new TypeReference[List[HistoricalServerInfo]] {})
  }

  @throws[DruidDataSourceException]
  def dataSourceInfo(dataSource: String): DataSourceSegmentInfo = {
    val url = s"$urlPrefix/datasources/$dataSource?full=true"
    val resp: String = get(url)
    jsonMapper.readValue(resp, new TypeReference[DataSourceSegmentInfo] {})
  }
}
