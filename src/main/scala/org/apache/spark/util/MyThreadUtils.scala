package org.apache.spark.util

import java.util.concurrent.ThreadPoolExecutor

object MyThreadUtils {

  def newDaemonCachedThreadPool(prefix: String, maxThreadNumber: Int,
                                keepAliveSeconds: Int = 60): ThreadPoolExecutor = {
    ThreadUtils.newDaemonCachedThreadPool(prefix, maxThreadNumber, keepAliveSeconds)
  }

  def addShutdownHook(hook: () => Unit): Unit = {
    ShutdownHookManager.addShutdownHook(hook)
  }
}
