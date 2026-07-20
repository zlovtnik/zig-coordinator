package com.sslproxy.coordinator.tidb

import cats.effect.IO
import org.slf4j.LoggerFactory

class TidbSchemaPreflight(transactor: TidbTransactor, config: TidbConfig):
  import TidbSchemaPreflight.log

  private val allRequiredTables: List[String] = List(
    "proxy_events",
    "proxy_blocked_host_rollups",
    "proxy_payload_audit",
    "wireless_sensors",
    "wireless_audit_frames",
    "wireless_bandwidth_windows",
    "wireless_alerts",
    "wireless_client_inventory",
    "wireless_probe_requests"
  )

  def validate(): IO[Unit] =
    if !config.enabled then IO.unit
    else
      transactor.preflightCheck(allRequiredTables).flatMap { missing =>
        if missing.isEmpty then
          IO(log.info("event=tidb_schema_preflight status=ok tables={}", allRequiredTables.size))
        else
          val msg = s"TiDB sink schema objects unavailable: missing=[${missing.mkString(", ")}]; " +
            s"apply tidb/init/001_schema.sql to the TiDB database '${config.database}'"
          if config.warnOnly then
            IO(log.warn("event=tidb_schema_preflight status=warn_only error=\"{}\"", sanitize(msg)))
          else
            IO.raiseError(IllegalStateException(msg))
      }

  private def sanitize(msg: String): String =
    if msg == null then "" else msg.replace('\n', ' ').replace('\r', ' ')

object TidbSchemaPreflight:
  private val log = LoggerFactory.getLogger(getClass)
