package com.sslproxy.coordinator.sink

import cats.effect.IO
import com.sslproxy.coordinator.config.SystemRegistryConfig
import com.sslproxy.coordinator.model.SystemContext
import org.slf4j.LoggerFactory

final class SystemRegistry(config: SystemRegistryConfig):
  import SystemRegistry.log

  private val knownOrigins: Set[String] = config.knownOrigins.map(_.toLowerCase(java.util.Locale.ROOT)).toSet

  def validate(ctx: SystemContext): IO[Unit] =
    val originLower = ctx.origin.toLowerCase(java.util.Locale.ROOT)
    if knownOrigins.contains(originLower) then IO.unit
    else
      val err = s"unknown origin '${ctx.origin}' not in known origins: ${knownOrigins.mkString(", ")}"
      log.warn("event=system_registry status=unknown_origin origin={} known={}", ctx.origin, knownOrigins.mkString(","))
      IO.raiseError(IllegalArgumentException(err))

object SystemRegistry:
  private val log = LoggerFactory.getLogger(getClass)
