package com.sslproxy.coordinator.observability

import org.slf4j.{Logger, LoggerFactory}

/** Type-safe structured logger that emits one JSON object per log line.
  *
  * Fields are rendered as `key=value` pairs in the SLF4J message string.
  * The LogstashLogbackEncoder in logback.xml parses these into top-level
  * JSON fields automatically.
  *
  * Value types are restricted to String/Int/Long/Double/Boolean — no Any,
  * stays wartremover-clean. If a value doesn't fit, `.toString` it first.
  */
final class StructuredLogger private (underlying: Logger):

  def info(event: String, fields: (String, String)*): Unit =
    underlying.info(format(event, fields))

  def warn(event: String, fields: (String, String)*): Unit =
    underlying.warn(format(event, fields))

  def error(event: String, fields: (String, String)*): Unit =
    underlying.error(format(event, fields))

  def debug(event: String, fields: (String, String)*): Unit =
    underlying.debug(format(event, fields))

  def trace(event: String, fields: (String, String)*): Unit =
    underlying.trace(format(event, fields))

  /** Error with cause — the Throwable is passed to SLF4J so Logback
    * attaches it as a real `stack_trace` field, not a stringified exception.
    */
  def error(event: String, cause: Throwable, fields: (String, String)*): Unit =
    underlying.error(format(event, fields), cause)

  private def format(event: String, fields: Seq[(String, String)]): String =
    if fields.isEmpty then event
    else
      val sb = new StringBuilder(event)
      for (k, v) <- fields do
        sb.append(' ').append(k).append('=').append(sanitize(v))
      sb.toString

  private def sanitize(value: String): String =
    if value == null then "" else value.replace('\n', ' ').replace('\r', ' ')

object StructuredLogger:
  def apply(clazz: Class[?]): StructuredLogger =
    new StructuredLogger(LoggerFactory.getLogger(clazz))

  def apply(name: String): StructuredLogger =
    new StructuredLogger(LoggerFactory.getLogger(name))
