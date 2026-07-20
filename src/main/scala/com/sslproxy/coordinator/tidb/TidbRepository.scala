package com.sslproxy.coordinator.tidb

import cats.effect.IO
import cats.effect.implicits.*
import cats.syntax.all.*
import com.sslproxy.coordinator.domain.{DatabaseError, ScanRequestRecord, SyncLoad}
import doobie.*
import doobie.implicits.*
import io.circe.{Json, parser as circeParser}
import org.slf4j.LoggerFactory
import scala.annotation.nowarn

class TidbRepository(xa: Transactor[IO]):
  import TidbRepository.log

  def checkConnectivity(): IO[Either[DatabaseError, Unit]] =
    runDb("tidb.check_connectivity") {
      sql"SELECT 1".query[Int].unique.map(_ => ())
    }

  def pendingLedgerCount(): IO[Either[DatabaseError, Long]] =
    runDb("tidb.pending_ledger_count") {
      sql"""SELECT COUNT(*) FROM sync_events
            WHERE status IN ('pending', 'processing')"""
        .query[Long]
        .unique
    }

  @nowarn("msg=unused explicit parameter")
  def processIngestLedger(
      streamNames: List[String],
      loadStreamNames: List[String],
      scanMaxAttempts: Int,
      scanRetryBackoffSeconds: Int,
      ingestBatchSize: Int
  ): IO[Either[DatabaseError, Long]] =
    runDb("tidb.process_ingest_ledger") {
      val limit = ingestBatchSize max 1
      val backoffSecs = scanRetryBackoffSeconds max 1

      for
        _ <- sql"""UPDATE sync_events e
                   SET e.status = 'batched', e.updated_at = NOW(6)
                   WHERE e.status = 'processing'
                     AND EXISTS (SELECT 1 FROM sync_batches b
                                  WHERE b.dedupe_key = e.dedupe_key AND b.stream_name = e.stream_name)""".update.run
        _ <- sql"""UPDATE sync_events
                   SET status = 'failed', updated_at = NOW(6),
                       last_error = 'coordinator processing lease expired'
                   WHERE status = 'processing'
                     AND updated_at < NOW(6) - INTERVAL $backoffSecs SECOND
                     AND NOT EXISTS (SELECT 1 FROM sync_batches b
                                     WHERE b.dedupe_key = sync_events.dedupe_key AND b.stream_name = sync_events.stream_name)""".update.run
        processed <- markProcessing(streamNames, maxAttempts = scanMaxAttempts, limit, backoffSecs)
      yield processed.toLong
    }

  private def markProcessing(
      streamNames: List[String], maxAttempts: Int, limit: Int, backoffSecs: Int
  ): ConnectionIO[Int] =
    if streamNames.isEmpty then 0.pure[ConnectionIO]
    else
      val inClause = streamNames.map(sn => fr0"$sn").intercalate(fr",")
      val whereIn = fr"e.stream_name IN (" ++ inClause ++ fr")"
      val whereFailed = fr"(e.status = 'pending' OR e.updated_at <= NOW(6) - INTERVAL $backoffSecs SECOND)"

      (sql"""UPDATE sync_events e
             SET e.status = 'processing',
                 e.attempt_count = e.attempt_count + 1,
                 e.updated_at = NOW(6),
                 e.last_error = NULL
             WHERE (e.dedupe_key, e.stream_name) IN (
               SELECT dedupe_key, stream_name FROM (
                 SELECT se.dedupe_key, se.stream_name
                 FROM sync_events se
                 WHERE se.status IN ('pending', 'failed')
                   AND """ ++ whereIn ++
             sql""" AND se.attempt_count < $maxAttempts
                   AND """ ++ whereFailed ++
             sql""" ORDER BY se.observed_at ASC
                   LIMIT $limit
                   FOR UPDATE SKIP LOCKED
               ) sub
             )""").update.run

  def recordScanRequests(records: List[ScanRequestRecord]): IO[Either[DatabaseError, Int]] =
    if records.isEmpty then IO.pure(Right(0))
    else
      runDb("tidb.record_scan_request") {
        val chunks = records.grouped(500).toList
        chunks.traverse(recordScanRequestChunk).map(_.sum)
      }

  private def recordScanRequestChunk(chunk: List[ScanRequestRecord]): ConnectionIO[Int] =
    val seen = scala.collection.mutable.Set.empty[(String, String)]
    val sql =
      """INSERT INTO sync_events (dedupe_key, stream_name, observed_at, payload_ref, payload,
         payload_sha256, status, attempt_count, last_error, producer, event_kind, created_at, updated_at)
       VALUES (?, ?, IFNULL(?, NOW(6)), ?, ?, ?, 'pending', 0, NULL, 'ssl-proxy', ?, NOW(6), NOW(6))
       ON DUPLICATE KEY UPDATE
         observed_at = IFNULL(VALUES(observed_at), observed_at),
         payload_ref = VALUES(payload_ref),
         payload = COALESCE(VALUES(payload), payload),
         payload_sha256 = VALUES(payload_sha256),
         producer = VALUES(producer),
         status = CASE WHEN sync_events.status IN ('pending', 'failed') THEN 'pending' ELSE sync_events.status END,
         last_error = CASE WHEN sync_events.status IN ('pending', 'failed') THEN NULL ELSE sync_events.last_error END,
         updated_at = NOW(6)"""

    val params: List[(String, String, Option[java.sql.Timestamp], String, String, String, Option[String])] =
      chunk.flatMap { r =>
        if seen.contains((r.dedupeKey, r.streamName)) then None
        else
          seen.add((r.dedupeKey, r.streamName))
          val payloadRef = circeParser.parse(r.requestJson).toOption
            .flatMap(_.hcursor.get[String]("payload_ref").toOption).orNull
          val eventKind = circeParser.parse(r.payloadJson).toOption
            .flatMap(_.hcursor.get[String]("type").toOption).orNull
          val observed = if r.observedAt.nonEmpty then parseTs(r.observedAt) else None
          if r.dedupeKey.isBlank || payloadRef == null || payloadRef.isBlank then None
          else Some((r.dedupeKey, r.streamName, observed, payloadRef, r.payloadJson, r.payloadSha256, Option(eventKind)))
      }

    Update[(String, String, Option[java.sql.Timestamp], String, String, String, Option[String])](sql)
      .updateMany(params)

  def getNextBatch(loadStreamNames: List[String]): IO[Either[DatabaseError, Option[SyncLoad]]] =
    runDb("tidb.get_next_batch") {
      if loadStreamNames.isEmpty then None.pure[ConnectionIO]
      else
        val inClause = loadStreamNames.map(sn => fr0"$sn").intercalate(fr",")
        val whereIn = fr"j.stream_name IN (" ++ inClause ++ fr")"

        val pickOne =
          (sql"""SELECT b.batch_id, b.job_id,
                        COALESCE(b.payload_ref, '') AS payload_ref,
                        COALESCE(e.payload, '') AS stored_payload,
                        COALESCE(b.cursor_start, '') AS cursor_start,
                        COALESCE(b.cursor_end, '') AS cursor_end,
                        CAST(b.attempt_count AS SIGNED INTEGER) AS attempt_count,
                        j.stream_name
                 FROM sync_batches b
                 JOIN sync_jobs j ON j.job_id = b.job_id
                 LEFT JOIN sync_events e ON e.dedupe_key = b.dedupe_key AND e.stream_name = b.stream_name
                 WHERE b.status = 'pending'
                   AND """ ++ whereIn ++
               sql""" ORDER BY b.batch_id
                 LIMIT 1
                 FOR UPDATE SKIP LOCKED""").query[(String, String, String, String, String, String, Int, String)].option

        pickOne.flatMap {
          case None => none.pure[ConnectionIO]
          case Some((batchId, jobId, payloadRef, storedPayload, cursorStart, cursorEnd, attemptCount, streamName)) =>
            val resolvedPayloadRef =
              if payloadRef.nonEmpty then payloadRef
              else if storedPayload.nonEmpty then
                val encoded = java.util.Base64.getUrlEncoder.withoutPadding.encodeToString(
                  storedPayload.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                )
                s"inline://json/$encoded"
              else throw new RuntimeException(s"batch $batchId: payload_ref missing and no stored payload")

            val markBatch: ConnectionIO[Unit] =
              (sql"""UPDATE sync_batches
                     SET status = 'dispatched',
                         attempt_count = attempt_count + 1,
                         last_error = NULL,
                         updated_at = NOW(6),
                         payload_ref = $resolvedPayloadRef
                     WHERE batch_id = $batchId AND status = 'pending'""").update.run.void

            val markJob: ConnectionIO[Unit] =
              (sql"""UPDATE sync_jobs
                     SET status = 'running',
                         started_at = COALESCE(started_at, NOW(6))
                     WHERE job_id = $jobId""").update.run.void

            markBatch *> markJob *>
              SyncLoad(
                jobId = jobId,
                batchId = batchId,
                batchNo = Some(0),
                streamName = streamName,
                payloadRef = resolvedPayloadRef,
                cursorStart = cursorStart,
                cursorEnd = cursorEnd,
                attempt = attemptCount + 1
              ).some.pure[ConnectionIO]
        }
    }

  def recoverStaleDispatchedBatches(
      loadStreamNames: List[String],
      leaseSeconds: Int,
      maxAttempts: Int
  ): IO[Either[DatabaseError, Int]] =
    runDb("tidb.recover_stale_dispatched_batches") {
      if loadStreamNames.isEmpty then 0.pure[ConnectionIO]
      else
        val inClause = loadStreamNames.map(sn => fr0"$sn").intercalate(fr",")
        val whereIn = fr"j.stream_name IN (" ++ inClause ++ fr")"
        val vMaxAttempts = maxAttempts max 1

        for
          staleCount <- (sql"""UPDATE sync_batches b
                               JOIN sync_jobs j ON j.job_id = b.job_id
                               SET b.status = CASE WHEN b.attempt_count >= $vMaxAttempts THEN 'failed' ELSE 'pending' END,
                                   b.last_error = CASE WHEN b.attempt_count >= $vMaxAttempts
                                                     THEN 'dispatch lease expired; max attempts reached'
                                                     ELSE 'dispatch lease expired; retrying' END,
                                   b.updated_at = NOW(6)
                               WHERE b.status = 'dispatched'
                                 AND b.updated_at < NOW(6) - INTERVAL $leaseSeconds SECOND
                                 AND """ ++ whereIn).update.run
          _ <- (sql"""UPDATE sync_jobs j
                      JOIN sync_batches b ON b.job_id = j.job_id
                      SET j.status = CASE WHEN b.status = 'failed' THEN 'failed' ELSE 'pending' END,
                          j.finished_at = CASE WHEN b.status = 'failed' THEN NOW(6) ELSE NULL END
                      WHERE b.status IN ('failed', 'pending')
                        AND b.updated_at >= NOW(6) - INTERVAL 2 SECOND""").update.run
        yield staleCount
    }

  def markBatchDispatchFailed(
      loadJson: String,
      errorText: String,
      maxAttempts: Int
  ): IO[Either[DatabaseError, Unit]] =
    runDb("tidb.mark_batch_dispatch_failed") {
      val parsed = circeParser.parse(loadJson).getOrElse(Json.Null)
      val batchId = parsed.hcursor.get[String]("batch_id").getOrElse("")
      val vMaxAttempts = maxAttempts max 1

      if batchId.isEmpty then
        (sql"""INSERT INTO sync_errors (job_id, batch_id, error_class, error_text)
               VALUES (NULL, NULL, 'dispatch_publish_failed', $errorText)""").update.run.void
      else
        for
          _ <- (sql"""UPDATE sync_batches
                      SET attempt_count = attempt_count + 1,
                          status = CASE WHEN attempt_count + 1 >= $vMaxAttempts THEN 'failed' ELSE 'pending' END,
                          last_error = $errorText,
                          updated_at = NOW(6)
                      WHERE batch_id = $batchId AND status = 'dispatched'""").update.run
          _ <- (sql"""UPDATE sync_jobs j
                      JOIN sync_batches b ON b.job_id = j.job_id
                      SET j.status = CASE WHEN b.status = 'failed' THEN 'failed' ELSE j.status END,
                          j.finished_at = CASE WHEN b.status = 'failed' THEN NOW(6) ELSE j.finished_at END
                      WHERE b.batch_id = $batchId""").update.run
        yield ()
    }

  def releaseBatchDispatch(
      loadJson: String,
      errorText: String
  ): IO[Either[DatabaseError, Unit]] =
    runDb("tidb.release_batch_dispatch") {
      val parsed = circeParser.parse(loadJson).getOrElse(Json.Null)
      val batchId = parsed.hcursor.get[String]("batch_id").getOrElse("")

      if batchId.isEmpty then ().pure[ConnectionIO]
      else
        for
          _ <- (sql"""UPDATE sync_batches
                      SET attempt_count = GREATEST(attempt_count - 1, 0),
                          status = 'pending',
                          last_error = $errorText,
                          updated_at = NOW(6)
                      WHERE batch_id = $batchId AND status = 'dispatched'""").update.run
          _ <- (sql"""UPDATE sync_jobs j
                      JOIN sync_batches b ON b.job_id = j.job_id
                      SET j.status = 'pending',
                          j.finished_at = NULL
                      WHERE b.batch_id = $batchId""").update.run
        yield ()
    }

  def ensureCursor(streamName: String): IO[Either[DatabaseError, String]] =
    runDb("tidb.ensure_cursor") {
      for
        _ <- sql"""INSERT INTO sync_cursors (stream_name, cursor_value, updated_at)
                   VALUES ($streamName, '0', NOW(6))
                   ON DUPLICATE KEY UPDATE updated_at = NOW(6)""".update.run
        cursor <- sql"""SELECT cursor_value FROM sync_cursors WHERE stream_name = $streamName"""
          .query[String].unique
      yield cursor
    }

  def ensureAllCursors(streamNames: List[String], parallelism: Int = 1): IO[Either[DatabaseError, String]] =
    if streamNames.isEmpty then IO.pure(Right("ok"))
    else
      streamNames.parTraverseN(parallelism.max(1)) { name =>
        runDb(s"tidb.ensure_cursor_$name") {
          sql"""INSERT INTO sync_cursors (stream_name, cursor_value, updated_at)
                VALUES ($name, '0', NOW(6))
                ON DUPLICATE KEY UPDATE updated_at = NOW(6)""".update.run
        }
      }.map { results =>
        if results.exists(_.isLeft) then
          results.collectFirst { case Left(e) => Left(e) }.get
        else
          Right("ok")
      }

  private val windowSecs = 60
  private val signalThreshold = -50
  private val presenceWindowSecs = 300

  def generateShadowAlerts(): IO[Either[DatabaseError, List[String]]] =
    runDb("tidb.generate_shadow_alerts") {
      (fr"""INSERT INTO wireless_shadow_alerts (
            source_mac, first_occurred_at, last_occurred_at, occurrence_count,
            destination_bssid, ssid, sensor_id, location_id, signal_dbm,
            reason, evidence, created_at, updated_at
          )
          SELECT
            w.source_mac, w.observed_at, w.observed_at, 1,
            w.destination_bssid, w.ssid, w.sensor_id, w.location_id, w.signal_dbm,
            'strong_wireless_without_proxy_presence',
            JSON_OBJECT('window_seconds', 60, 'signal_threshold_dbm', -50, 'presence_window_seconds', 300),
            NOW(6), NOW(6)
          FROM (
            SELECT DISTINCT
              LOWER(e.payload->>'$$.source_mac') AS source_mac,
              e.observed_at,
              LOWER(COALESCE(NULLIF(TRIM(e.payload->>'$$.destination_bssid'), ''), NULLIF(TRIM(e.payload->>'$$.bssid'), ''))) AS destination_bssid,
              e.payload->>'$$.ssid' AS ssid,
              COALESCE(e.payload->>'$$.sensor_id', e.payload->>'$$.sensor_id') AS sensor_id,
              COALESCE(e.payload->>'$$.location_id', e.payload->>'$$.location_id') AS location_id,
              CAST(e.payload->>'$$.signal_dbm' AS SIGNED) AS signal_dbm
            FROM sync_events e
            WHERE e.stream_name = 'wireless.audit'
              AND e.observed_at >= NOW(6) - """ ++ Fragment.const(s"INTERVAL $windowSecs SECOND") ++
            fr""" AND e.payload IS NOT NULL
          ) w
          WHERE w.source_mac IS NOT NULL
            AND w.source_mac REGEXP '^[0-9a-f]{2}(:[0-9a-f]{2}){5}$$'
            AND w.signal_dbm >= $signalThreshold
            AND NOT EXISTS (
              SELECT 1 FROM wireless_authorized_networks awn
              WHERE awn.enabled = TRUE
                AND (awn.location_id IS NULL OR awn.location_id = w.location_id)
                AND (awn.ssid IS NULL OR (w.ssid IS NOT NULL AND awn.ssid = w.ssid))
                AND (awn.bssid IS NULL OR (w.destination_bssid IS NOT NULL AND awn.bssid = w.destination_bssid))
            )
            AND NOT EXISTS (
              SELECT 1 FROM devices d
              WHERE d.mac_id = w.source_mac AND d.last_seen >= NOW(6) - """ ++ Fragment.const(s"INTERVAL $presenceWindowSecs SECOND") ++
            fr"""
          )
          ON DUPLICATE KEY UPDATE
            last_occurred_at = GREATEST(wireless_shadow_alerts.last_occurred_at, VALUES(last_occurred_at)),
            occurrence_count = wireless_shadow_alerts.occurrence_count + 1,
            destination_bssid = IF(VALUES(last_occurred_at) >= wireless_shadow_alerts.last_occurred_at, VALUES(destination_bssid), wireless_shadow_alerts.destination_bssid),
            ssid = IF(VALUES(last_occurred_at) >= wireless_shadow_alerts.last_occurred_at, VALUES(ssid), wireless_shadow_alerts.ssid),
            sensor_id = IF(VALUES(last_occurred_at) >= wireless_shadow_alerts.last_occurred_at, VALUES(sensor_id), wireless_shadow_alerts.sensor_id),
            location_id = IF(VALUES(last_occurred_at) >= wireless_shadow_alerts.last_occurred_at, VALUES(location_id), wireless_shadow_alerts.location_id),
            signal_dbm = IF(VALUES(last_occurred_at) >= wireless_shadow_alerts.last_occurred_at, VALUES(signal_dbm), wireless_shadow_alerts.signal_dbm),
            updated_at = NOW(6)""").update.run *>
        (fr"""SELECT JSON_OBJECT(
              'event_type', 'shadow_device',
              'first_occurred_at', first_occurred_at,
              'last_occurred_at', last_occurred_at,
              'source_mac', source_mac,
              'occurrence_count', occurrence_count,
              'destination_bssid', destination_bssid,
              'ssid', ssid,
              'sensor_id', sensor_id,
              'location_id', location_id,
              'signal_dbm', signal_dbm,
              'reason', reason,
              'evidence', evidence
            ) AS alert_json
            FROM wireless_shadow_alerts
            WHERE updated_at >= NOW(6) - INTERVAL 2 SECOND""").query[String].to[List]
    }

  def lookupDeviceByMac(mac: String): IO[Either[DatabaseError, Option[String]]] =
    runDb("tidb.lookup_device_by_mac") {
      sql"""SELECT JSON_OBJECT(
              'device_id', mac_id,
              'username', username,
              'display_name', display_name,
              'hostname', hostname
            ) AS device_json
            FROM devices
            WHERE LOWER(mac_id) = LOWER($mac)
            LIMIT 1"""
        .query[String]
        .option
    }

  def listAuthorizedNetworks(): IO[Either[DatabaseError, String]] =
    runDb("tidb.list_authorized_networks") {
      sql"""SELECT COALESCE(
              JSON_ARRAYAGG(
                JSON_OBJECT(
                  'ssid', ssid,
                  'bssid', LOWER(bssid),
                  'location_id', location_id,
                  'label', label,
                  'enabled', enabled
                )
              ),
              '[]'
            ) AS networks_json
            FROM wireless_authorized_networks
            WHERE enabled = TRUE""".query[String].unique
    }

  def flushProbeBatch(probesJson: String): IO[Either[DatabaseError, Int]] =
    runDb("tidb.flush_probe_batch") {
      val parsed = circeParser.parse(probesJson).getOrElse(Json.Null)
      val probes = parsed.asArray.getOrElse(
        parsed.hcursor.downField("probes").as[Vector[Json]].getOrElse(Vector.empty)
      )

      if probes.isEmpty then 0.pure[ConnectionIO]
      else
        val batchId = java.security.MessageDigest.getInstance("MD5")
          .digest(probesJson.getBytes(java.nio.charset.StandardCharsets.UTF_8))
          .map("%02x".format(_)).mkString

        probes.traverse { probe =>
          val ssid = probe.hcursor.get[String]("ssid").getOrElse("")
          val clientMac = probe.hcursor.get[String]("client_mac").getOrElse("")
          val firstSeen = probe.hcursor.get[String]("first_seen").toOption.flatMap(parseTs).orNull
          val lastSeen = probe.hcursor.get[String]("last_seen").toOption.flatMap(parseTs).orNull
          val probeCount = probe.hcursor.get[Long]("probe_count").getOrElse(1L)
          val locationId = probe.hcursor.get[String]("location_id").toOption.orNull
          val observedBssid = probe.hcursor.get[String]("observed_bssid").toOption
            .orElse(probe.hcursor.get[String]("known_bssid").toOption)
            .orElse(probe.hcursor.get[String]("bssid").toOption).orNull

          sql"""INSERT INTO wireless_clients (ssid, client_mac, known_bssid, first_seen, last_seen,
                  probe_count, location_id, last_probe_batch_id)
                VALUES ($ssid, $clientMac,
                  (SELECT MAX(authorized.bssid) FROM wireless_authorized_networks authorized
                   WHERE LOWER(authorized.ssid) = LOWER($ssid) AND authorized.enabled = TRUE
                     AND ($observedBssid IS NULL OR LOWER(authorized.bssid) = LOWER($observedBssid))
                     AND ($locationId IS NULL OR authorized.location_id = $locationId)
                   HAVING COUNT(*) = 1),
                  $firstSeen, $lastSeen, $probeCount,
                  $locationId, $batchId)
                ON DUPLICATE KEY UPDATE
                  first_seen = LEAST(wireless_clients.first_seen, VALUES(first_seen)),
                  last_seen = GREATEST(wireless_clients.last_seen, VALUES(last_seen)),
                  probe_count = CASE
                    WHEN wireless_clients.last_probe_batch_id IS NULL
                      OR wireless_clients.last_probe_batch_id != VALUES(last_probe_batch_id)
                    THEN wireless_clients.probe_count + VALUES(probe_count)
                    ELSE wireless_clients.probe_count
                  END,
                  known_bssid = COALESCE(VALUES(known_bssid), wireless_clients.known_bssid),
                  location_id = COALESCE(VALUES(location_id), wireless_clients.location_id),
                  last_probe_batch_id = VALUES(last_probe_batch_id)""".update.run
        }.map(_.sum)
    }

  private def runDb[A](operation: String)(fa: ConnectionIO[A]): IO[Either[DatabaseError, A]] =
    fa.transact(xa)
      .map(Right(_))
      .handleError { cause =>
        log.error("event=db_error operation={} error=\"{}\"", operation, sanitize(cause.getMessage))
        Left(DatabaseError.Retryable(operation, cause, cause.getMessage))
      }

  private def sanitize(msg: String): String =
    if msg == null then "" else msg.replace('\n', ' ').replace('\r', ' ')

  private def parseTs(s: String): Option[java.sql.Timestamp] =
    try
      Some(java.sql.Timestamp.from(java.time.Instant.parse(s)))
    catch case _: Exception =>
      try
        Some(java.sql.Timestamp.valueOf(s.replace("T", " ").substring(0, 19)))
      catch case _: Exception => None

object TidbRepository:
  private val log = LoggerFactory.getLogger(getClass)
