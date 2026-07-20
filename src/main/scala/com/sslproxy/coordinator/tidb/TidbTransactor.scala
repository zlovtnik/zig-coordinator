package com.sslproxy.coordinator.tidb

import cats.effect.{IO, Resource}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.slf4j.LoggerFactory

import java.sql.{Connection, PreparedStatement, Timestamp, Types}
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

/** HikariCP-backed TiDB transactor implementing TidbSink. Ported from OracleTransactor. */
final class TidbTransactor private (ds: HikariDataSource) extends TidbSink:
  import TidbTransactor.log

  private val batchSize: Int = 500

  private def withConnection[A](f: Connection => A): IO[A] =
    IO.blocking {
      val conn = ds.getConnection
      try f(conn)
      finally conn.close()
    }

  private def executeBatch(stmt: PreparedStatement, rows: Seq[Seq[Any]]): Long =
    var count = 0L
    for row <- rows do
      for (value, idx) <- row.zipWithIndex.to(List) do
        setParam(stmt, idx + 1, value)
      stmt.addBatch()
      count += 1
      if count % batchSize == 0 then stmt.executeBatch(): Unit
    if count % batchSize != 0 then stmt.executeBatch(): Unit
    count

  private def setParam(stmt: PreparedStatement, idx: Int, value: Any): Unit =
    value match
      case null              => stmt.setNull(idx, Types.NULL)
      case v: String         => stmt.setString(idx, v)
      case v: Int            => stmt.setInt(idx, v)
      case v: Long           => stmt.setLong(idx, v)
      case v: Boolean        => stmt.setBoolean(idx, v)
      case v: Double         => stmt.setDouble(idx, v)
      case v: Timestamp      => stmt.setTimestamp(idx, v)
      case v: java.sql.Date  => stmt.setDate(idx, v)
      case Some(inner)       => setParam(stmt, idx, inner)
      case None              => stmt.setNull(idx, Types.NULL)
      case v: OffsetDateTime => stmt.setString(idx, v.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
      case v                 => stmt.setString(idx, v.toString)

  private def ts(odt: OffsetDateTime): Timestamp =
    Timestamp.from(odt.toInstant)

  private def optLong(v: Option[Long]): java.lang.Long = v.map(java.lang.Long.valueOf).orNull
  private def optStr(v: Option[String]): String = v.orNull
  private def optDbl(v: Option[Double]): java.lang.Double = v.map(java.lang.Double.valueOf).orNull

  // ── proxy_events ──────────────────────────────────────────────
  override def insertProxyEvents(
      batchId: String,
      rows: List[ProxyEventInsert],
      blockedRows: List[BlockedEventInsert]
  ): IO[Long] =
    withConnection { conn =>
      val sql =
        """INSERT INTO proxy_events (
          |  batch_id, row_sequence, event_timestamp_utc, event_time, event_type, host,
          |  peer_ip, wg_pubkey, device_id, identity_source, peer_hostname, client_ua,
          |  bytes_up, bytes_down, status_code, blocked, obfuscation_profile,
          |  correlation_id, parent_event_id, event_sequence, duration_ms, reason, raw_json
          |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          |ON DUPLICATE KEY UPDATE batch_id = VALUES(batch_id)""".stripMargin

      val stmt = conn.prepareStatement(sql)
      try
        val allRows = rows.map(r =>
          Seq[Any](
            batchId, 0L,
            ts(r.eventTime), ts(r.eventTime),
            r.eventType, r.host,
            optStr(r.peerIp), optStr(r.wgPubkey),
            optStr(r.deviceId), r.identitySource,
            optStr(r.peerHostname), optStr(r.clientUa),
            r.bytesUp, r.bytesDown,
            optLong(r.statusCode), r.blocked,
            optStr(r.obfuscationProfile),
            optStr(r.correlationId), optLong(r.parentEventId.map(_.toLong)),
            optLong(r.eventSequence), optLong(r.durationMs),
            optStr(r.reason), optStr(r.rawJson)
          )
        )
        executeBatch(stmt, allRows)
      finally stmt.close()
    }

  // ── proxy_blocked_host_rollups ────────────────────────────────
  override def insertBlockedHostRollups(batchId: String, rows: List[BlockedEventInsert]): IO[Long] =
    withConnection { conn =>
      val sql =
        """INSERT INTO proxy_blocked_host_rollups (
          |  host, blocked_attempts, blocked_bytes, frequency_hz, verdict, category,
          |  risk_score, tarpit_held_ms, iat_ms, consecutive_blocks, last_verdict,
          |  tls_ver, alpn, ja3_lite, resolved_ip, asn_org, updated_at, first_seen
          |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          |ON DUPLICATE KEY UPDATE host = VALUES(host)""".stripMargin

      val stmt = conn.prepareStatement(sql)
      try
        val now = Timestamp.from(java.time.Instant.now())
        val params = rows.map(r =>
          Seq[Any](
            r.host, r.blockedBytes, r.blockedBytes,
            optDbl(r.frequencyHz), r.verdict, r.category,
            optDbl(r.riskScore), r.tarpitHeldMs,
            optLong(r.iatMs), optLong(r.consecutiveBlocks),
            r.lastVerdict, optStr(r.tlsVer), optStr(r.alpn),
            optStr(r.ja3Lite), optStr(r.resolvedIp), optStr(r.asnOrg),
            now, now
          )
        )
        executeBatch(stmt, params)
      finally stmt.close()
    }

  // ── proxy_payload_audit ───────────────────────────────────────
  override def insertProxyPayloadAudit(batchId: String, rows: List[ProxyPayloadAuditInsert]): IO[Long] =
    withConnection { conn =>
      val sql =
        """INSERT INTO proxy_payload_audit (
          |  correlation_id, host, direction, captured_at, byte_offset,
          |  payload_object_key, content_type, http_method, http_status, http_path,
          |  is_encrypted, truncated, peer_ip, notes
          |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          |ON DUPLICATE KEY UPDATE batch_id = ?""".stripMargin

      val stmt = conn.prepareStatement(sql)
      try
        val params = rows.map(r =>
          Seq[Any](
            r.correlationId, r.host, r.direction, ts(r.capturedAt), r.byteOffset,
            optStr(r.payloadObjectKey), optStr(r.contentType),
            optStr(r.httpMethod), optLong(r.httpStatus), optStr(r.httpPath),
            r.isEncrypted, r.truncated,
            optStr(r.peerIp), optStr(r.notes)
          )
        )
        executeBatch(stmt, params)
      finally stmt.close()
    }

  // ── wireless_audit_frames ─────────────────────────────────────
  override def insertWirelessAuditFrames(batchId: String, rows: List[WirelessAuditFrameInsert]): IO[Long] =
    withConnection { conn =>
      val sql =
        """INSERT INTO wireless_audit_frames (
          |  batch_id, row_sequence, event_type, observed_at, sensor_id, location_id,
          |  interface, channel, frame_type, frame_subtype, bssid, source_mac,
          |  destination_mac, transmitter_mac, receiver_mac, destination_bssid, ssid,
          |  signal_dbm, sequence_number, raw_len, is_retry, is_more_data, is_power_save,
          |  is_protected, is_to_ds, is_from_ds, is_handshake, security_flags,
          |  device_id, username, identity_source, tags, anomaly_reasons, raw_json, reg_domain
          |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          |ON DUPLICATE KEY UPDATE batch_id = VALUES(batch_id)""".stripMargin

      val stmt = conn.prepareStatement(sql)
      try
        val params = rows.map(r =>
          Seq[Any](
            batchId, r.rowSequence, r.eventType, ts(r.observedAt), r.sensorId, r.locationId,
            r.iface, r.channel,
            optStr(r.frameType), r.frameSubtype,
            optStr(r.bssid), optStr(r.sourceMac), optStr(r.destinationMac),
            optStr(r.transmitterMac), optStr(r.receiverMac), optStr(r.destinationBssid),
            optStr(r.ssid), optLong(r.signalDbm), optLong(r.sequenceNumber),
            r.rawLen, r.isRetry, r.isMoreData, r.isPowerSave,
            r.isProtected, r.isToDs, r.isFromDs, r.isHandshake,
            r.securityFlags, optStr(r.deviceId), optStr(r.username),
            r.identitySource, optStr(r.tags), optStr(r.anomalyReasons),
            optStr(r.rawJson), optStr(r.regDomain)
          )
        )
        executeBatch(stmt, params)
      finally stmt.close()
    }

  // ── wireless_bandwidth_windows ────────────────────────────────
  override def insertWirelessBandwidth(batchId: String, rows: List[WirelessBandwidthInsert]): IO[Long] =
    withConnection { conn =>
      val sql =
        """INSERT INTO wireless_bandwidth_windows (
          |  batch_id, row_sequence, schema_version, window_start, window_end,
          |  sensor_id, location_id, interface, channel, source_mac, destination_bssid,
          |  ssid, bytes, frame_count, retry_count, more_data_count, power_save_count,
          |  strongest_signal_dbm, hist_under_100, hist_100_500, hist_500_1000,
          |  hist_1000_1500, inter_arrival_p50_ms, external_bssid, threshold_exceeded,
          |  wall_clock_delta_ms, window_is_partial, published_at, created_at
          |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          |ON DUPLICATE KEY UPDATE batch_id = VALUES(batch_id)""".stripMargin

      val stmt = conn.prepareStatement(sql)
      try
        val params = rows.map(r =>
          Seq[Any](
            batchId, r.rowSequence, r.schemaVersion,
            ts(r.windowStart), ts(r.windowEnd),
            r.sensorId, r.locationId, r.iface, r.channel,
            r.sourceMac, r.destinationBssid,
            optStr(r.ssid), r.bytes, r.frameCount, r.retryCount,
            r.moreDataCount, r.powerSaveCount,
            optLong(r.strongestSignalDbm),
            r.histUnder100, r.hist100500, r.hist5001000, r.hist10001500,
            optLong(r.interArrivalP50Ms), r.externalBssid, r.thresholdExceeded,
            optLong(r.wallClockDeltaMs), r.windowIsPartial,
            r.publishedAt.map(ts).orNull, ts(r.windowStart)
          )
        )
        executeBatch(stmt, params)
      finally stmt.close()
    }

  // ── wireless_alerts ───────────────────────────────────────────
  override def insertWirelessAlerts(batchId: String, rows: List[WirelessRogueApInsert]): IO[Long] =
    withConnection { conn =>
      val sql =
        """INSERT INTO wireless_alerts (
          |  alert_type, batch_id, row_sequence, alert_date, detected_at, sensor_id,
          |  location_id, interface, channel, primary_mac, secondary_mac, ssid,
          |  signal_dbm, bytes, details_json, raw_json, acknowledged, acknowledged_by,
          |  acknowledged_at, created_at, updated_at
          |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          |ON DUPLICATE KEY UPDATE batch_id = VALUES(batch_id)""".stripMargin

      val stmt = conn.prepareStatement(sql)
      try
        val params = rows.map(r =>
          Seq[Any](
            "rogue_ap", batchId, r.rowSequence,
            java.sql.Date.valueOf(r.detectedAt.toLocalDate),
            ts(r.detectedAt), r.sensorId, r.locationId, r.iface, r.channel,
            r.rogueBssid, null,
            optStr(r.ssid), optLong(r.signalDbm), null,
            optStr(r.rawJson), optStr(r.rawJson),
            0L, null, null,
            ts(r.detectedAt), ts(r.detectedAt)
          )
        )
        executeBatch(stmt, params)
      finally stmt.close()
    }

  // ── wireless_alerts_ledger ────────────────────────────────────
  override def insertWirelessAlertsLedger(batchId: String, rows: List[WirelessRogueApInsert]): IO[Long] =
    withConnection { conn =>
      val sql =
        """INSERT INTO wireless_alerts_ledger (
          |  alert_pk, alert_type, batch_id, row_sequence, alert_date, detected_at,
          |  sensor_id, location_id, interface, channel, primary_mac, secondary_mac,
          |  ssid, signal_dbm, bytes, details_json, raw_json, acknowledged,
          |  ledger_action, captured_at
          |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          |ON DUPLICATE KEY UPDATE batch_id = VALUES(batch_id)""".stripMargin

      val stmt = conn.prepareStatement(sql)
      try
        val params = rows.map(r =>
          Seq[Any](
            0L, "rogue_ap", batchId, r.rowSequence,
            java.sql.Date.valueOf(r.detectedAt.toLocalDate),
            ts(r.detectedAt), r.sensorId, r.locationId, r.iface, r.channel,
            r.rogueBssid, null,
            optStr(r.ssid), optLong(r.signalDbm), null,
            optStr(r.rawJson), optStr(r.rawJson),
            0L, "INSERT", ts(r.detectedAt)
          )
        )
        executeBatch(stmt, params)
      finally stmt.close()
    }

  // ── wireless_client_inventory ─────────────────────────────────
  override def insertWirelessClientInventory(batchId: String, rows: List[WirelessClientInventoryInsert]): IO[Long] =
    withConnection { conn =>
      val sql =
        """INSERT INTO wireless_client_inventory (
          |  sensor_id, location_id, snapshot_at, client_mac, bssid, ssid,
          |  device_id, username, identity_source, last_seen, first_seen,
          |  signal_dbm, is_authorized, created_at
          |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          |ON DUPLICATE KEY UPDATE batch_id = ?""".stripMargin

      val stmt = conn.prepareStatement(sql)
      try
        val params = rows.map(r =>
          Seq[Any](
            r.sensorId, r.locationId, ts(r.snapshotAt),
            r.clientMac, optStr(r.bssid), optStr(r.ssid),
            optStr(r.deviceId), optStr(r.username), optStr(r.identitySource),
            ts(r.lastSeen), ts(r.firstSeen),
            optLong(r.signalDbm), r.isAuthorized,
            ts(r.snapshotAt)
          )
        )
        executeBatch(stmt, params)
      finally stmt.close()
    }

  // ── wireless_probe_requests ───────────────────────────────────
  override def insertWirelessProbeRequests(batchId: String, rows: List[WirelessProbeRequestInsert]): IO[Long] =
    withConnection { conn =>
      val sql =
        """INSERT INTO wireless_probe_requests (
          |  batch_id, row_sequence, client_mac, ssid, known_bssid,
          |  first_seen, last_seen, probe_count, created_at
          |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
          |ON DUPLICATE KEY UPDATE batch_id = VALUES(batch_id)""".stripMargin

      val stmt = conn.prepareStatement(sql)
      try
        val params = rows.map(r =>
          Seq[Any](
            batchId, r.rowSequence,
            r.clientMac, r.ssid, optStr(r.knownBssid),
            ts(r.firstSeen), ts(r.lastSeen),
            r.probeCount, ts(r.firstSeen)
          )
        )
        executeBatch(stmt, params)
      finally stmt.close()
    }

  def close(): IO[Unit] =
    IO.blocking {
      ds.close()
      log.info("TidbTransactor: HikariCP pool closed")
    }

object TidbTransactor:
  private val log = LoggerFactory.getLogger(getClass)

  def resource(config: TidbConfig): Resource[IO, TidbTransactor] =
    Resource.make(allocate(config))(_.close())

  private def allocate(config: TidbConfig): IO[TidbTransactor] =
    IO.blocking {
      val hikariConfig = new HikariConfig()
      hikariConfig.setJdbcUrl(TidbConfig.jdbcUrl(config))
      hikariConfig.setUsername(config.user)
      hikariConfig.setPassword(config.password)
      hikariConfig.setMaximumPoolSize(config.poolSize)
      hikariConfig.setConnectionTimeout(config.connectionTimeoutMs)
      hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver")
      hikariConfig.setPoolName("tidb-pool")
      hikariConfig.addDataSourceProperty("cachePrepStmts", "true")
      hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250")
      hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")

      val ds = new HikariDataSource(hikariConfig)
      log.info("TidbTransactor: HikariCP pool allocated to {}", config.host)
      new TidbTransactor(ds)
    }