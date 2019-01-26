package org.apache.spark.sql

import org.apache.spark.internal.Logging

trait MyLogging extends Logging {

  override def logInfo(msg: => String) = super.logInfo(msg)

  override def logDebug(msg: => String) = super.logDebug(msg)

  override def logTrace(msg: => String) = super.logTrace(msg)

  override def logWarning(msg: => String) = super.logWarning(msg)

  override def logError(msg: => String) = super.logError(msg)

  override def logInfo(msg: => String, throwable: Throwable) = super.logInfo(msg, throwable)

  override def logDebug(msg: => String, throwable: Throwable) = super.logDebug(msg, throwable)

  override def logTrace(msg: => String, throwable: Throwable) = super.logTrace(msg, throwable)

  override def logWarning(msg: => String, throwable: Throwable) = super.logWarning(msg, throwable)

  override def logError(msg: => String, throwable: Throwable) = super.logError(msg, throwable)
}
