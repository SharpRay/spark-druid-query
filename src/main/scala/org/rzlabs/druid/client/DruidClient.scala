package org.rzlabs.druid.client

import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.spark.sql.SQLContext

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
      init()
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
