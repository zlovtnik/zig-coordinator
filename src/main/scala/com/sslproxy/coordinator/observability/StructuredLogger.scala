package com.sslproxy.coordinator.observability

import net.logstash.logback.argument.StructuredArguments
import org.slf4j.{Logger, LoggerFactory}

/** Type-safe structured logger that emits one JSON object per log line.
  *
  * Fields are passed as SLF4J structured key-value arguments via
  * Logstash StructuredArguments so the LogstashLogbackEncoder in
  * logback.xml renders them as top-level JSON properties.
  *
  * Value types are restricted to String/Int/Long/Double/Boolean — no Any,
  * stays wartremover-clean. If a value doesn't fit, `.toString` it first.
  */
final class StructuredLogger private (underlying: Logger):

  def info(event: String, fields: (String, String)*): Unit =
    val args = fields.map { case (k, v) => StructuredArguments.keyValue(k, v) }.toArray
    underlying.info(event, args: _*)

  def warn(event: String, fields: (String, String)*): Unit =
    val args = fields.map { case (k, v) => StructuredArguments.keyValue(k, v) }.toArray
    underlying.warn(event, args: _*)

  def error(event: String, fields: (String, String)*): Unit =
    val args = fields.map { case (k, v) => StructuredArguments.keyValue(k, v) }.toArray
    underlying.error(event, args: _*)

  def debug(event: String, fields: (String, String)*): Unit =
    val args = fields.map { case (k, v) => StructuredArguments.keyValue(k, v) }.toArray
    underlying.debug(event, args: _*)

  def trace(event: String, fields: (String, String)*): Unit =
    val args = fields.map { case (k, v) => StructuredArguments.keyValue(k, v) }.toArray
    underlying.trace(event, args: _*)

  /** Error with cause — the Throwable is appended as the final vararg so
    * Logback attaches it as a real `stack_trace` field.
    */
  def error(event: String, cause: Throwable, fields: (String, String)*): Unit =
    val args = fields.map { case (k, v) => StructuredArguments.keyValue(k, v) }.toArray :+ cause
    underlying.error(event, args: _*)

object StructuredLogger:
  def apply(clazz: Class[?]): StructuredLogger =
    new StructuredLogger(LoggerFactory.getLogger(clazz))

  def apply(name: String): StructuredLogger =
    new StructuredLogger(LoggerFactory.getLogger(name))
