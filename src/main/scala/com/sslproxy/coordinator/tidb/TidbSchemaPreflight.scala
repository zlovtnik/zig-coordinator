package com.sslproxy.coordinator.tidb

import cats.effect.IO
import com.sslproxy.coordinator.config.TiDbConfig
import com.sslproxy.coordinator.observability.StructuredLogger

class TidbSchemaPreflight(transactor: TidbTransactor, config: TiDbConfig):
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
          IO(log.info("tidb_schema_preflight", "status" -> "ok", "tables" -> allRequiredTables.size.toString))
        else
          val msg = s"TiDB sink schema objects unavailable: missing=[${missing.mkString(", ")}]; " +
            s"apply tidb/init/001_schema.sql to the TiDB database '${config.database}'"
          if config.warnOnly then
            IO(log.warn("tidb_schema_preflight", "status" -> "warn_only", "error" -> msg))
          else
            IO.raiseError(IllegalStateException(msg))
      }

object TidbSchemaPreflight:
  private val log = StructuredLogger(getClass)
