package com.sslproxy.coordinator.tidb

import java.sql.{SQLException, SQLRecoverableException, SQLTransientException}
import scala.collection.mutable

enum TidbErrorClass(val wireValue: String):
  case Retryable extends TidbErrorClass("retryable")
  case Permanent extends TidbErrorClass("permanent")

object TidbErrorClass:

  def classify(failure: Throwable): TidbErrorClass =
    if failure == null then return Permanent

    val failures = mutable.ArrayDeque[Throwable](failure)
    val visited = mutable.Set.empty[Int]

    while failures.nonEmpty do
      val current = failures.removeHead()
      val identity = System.identityHashCode(current)
      if !visited.add(identity) then ()
      else
        current match
          case _: SQLRecoverableException | _: SQLTransientException =>
            return Retryable
          case sqlEx: SQLException =>
            if isRetryableSqlState(sqlEx.getSQLState) || isRetryableVendorCode(sqlEx.getErrorCode) then
              return Retryable
            if sqlEx.getNextException != null then
              failures += sqlEx.getNextException
          case _ => ()

        if isRetryableMessage(current.getMessage) then
          return Retryable

        val cause = current.getCause
        if cause != null && cause != current then
          failures += cause

    Permanent

  private def isRetryableSqlState(sqlState: String): Boolean =
    sqlState != null && {
      val normalized = sqlState.toUpperCase(java.util.Locale.ROOT)
      normalized.startsWith("08") ||
      normalized.startsWith("40") ||
      normalized == "HYT00" ||
      normalized == "HYT01" ||
      normalized == "70100" // MySQL: unknown SQLSTATE, often transient
    }

  private def isRetryableVendorCode(errorCode: Int): Boolean =
    Math.abs(errorCode) match
      // MySQL error codes that are retryable
      case 1205 | 1213 | 1218 | 2006 | 2013 | 2059 => true
      // TiDB-specific retryable codes
      case 8002 | 9007 | 9013 | 9014 => true
      case 40001 => true // serialization failure
      case _     => false

  private def isRetryableMessage(message: String): Boolean =
    val normalized = if message == null then "" else message.toLowerCase(java.util.Locale.ROOT)
    normalized.contains("timeout") ||
    normalized.contains("temporarily unavailable") ||
    normalized.contains("connection reset") ||
    normalized.contains("deadlock") ||
    normalized.contains("write conflict") ||
    normalized.contains("lock wait") ||
    normalized.contains("region unavailable")
