package com.sslproxy.coordinator.postgres

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.domain.{DatabaseError, ScanRequestRecord, SyncLoad}
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*
import org.slf4j.LoggerFactory

class CoordinatorRepository(xa: Transactor[IO]):
  import CoordinatorRepository.log

  private val SqlArrayChunkSize = 500

  def checkConnectivity(): IO[Either[DatabaseError, Unit]] =
    runDb("coordinator.check_connectivity") {
      sql"SELECT 1".query[Int].unique.map(_ => ())
    }

  def pendingLedgerCount(): IO[Either[DatabaseError, Long]] =
    runDb("coordinator.pending_ledger_count") {
      sql"""SELECT coordinator.pending_ledger_count()::text"""
        .query[String]
        .unique
        .map(parseLongOrZero)
    }

  def processIngestLedger(
      streamNames: List[String],
      loadStreamNames: List[String],
      scanMaxAttempts: Int,
      scanRetryBackoffSeconds: Int,
      ingestBatchSize: Int
  ): IO[Either[DatabaseError, Long]] =
    runDb("coordinator.process_ingest_ledger") {
      val streamNamesCsv = streamNames.mkString(",")
      val loadStreamNamesCsv = loadStreamNames.mkString(",")
      sql"""SELECT coordinator.process_ingest_ledger(
        string_to_array($streamNamesCsv::text, ','),
        string_to_array($loadStreamNamesCsv::text, ','),
        $scanMaxAttempts::integer,
        $scanRetryBackoffSeconds::integer,
        $ingestBatchSize::integer
      )::text"""
        .query[String]
        .unique
        .map(parseLongOrZero)
    }

  def recordScanRequests(records: List[ScanRequestRecord]): IO[Either[DatabaseError, Int]] =
    if records.isEmpty then IO.pure(Right(0))
    else
      runDb("coordinator.record_scan_request_batch") {
        val chunks = records.grouped(SqlArrayChunkSize).toList
        val results: ConnectionIO[List[Int]] = chunks.traverse(recordScanRequestChunk)
        results.map(_.sum)
      }

  private def recordScanRequestChunk(chunk: List[ScanRequestRecord]): ConnectionIO[Int] =
    val requestJsons = chunk.map(_.requestJson)
    val payloadJsons = chunk.map(_.payloadJson)
    val sha256s = chunk.map(_.payloadSha256)

    sql"""SELECT coordinator.record_scan_request_batch(
      $requestJsons::jsonb[],
      $payloadJsons::jsonb[],
      $sha256s::text[],
      ${List("proxy.payload_audit")}::text[]
    )::text"""
      .query[String]
      .unique
      .map(parseIntOrZero)

  def getNextBatch(loadStreamNames: List[String]): IO[Either[DatabaseError, Option[SyncLoad]]] =
    runDb("coordinator.get_next_batch") {
      val csv = loadStreamNames.mkString(",")
      sql"""SELECT coordinator.get_next_batch(string_to_array($csv::text, ','))::text"""
        .query[String]
        .option
        .map(_.flatMap(blankToNull).map(parseSyncLoad))
    }

  def recoverStaleDispatchedBatches(
      loadStreamNames: List[String],
      leaseSeconds: Int,
      maxAttempts: Int
  ): IO[Either[DatabaseError, Int]] =
    runDb("coordinator.recover_stale_dispatched_batches") {
      val csv = loadStreamNames.mkString(",")
      sql"""SELECT coordinator.recover_stale_dispatched_batches(
        string_to_array($csv::text, ','),
        $leaseSeconds::integer,
        $maxAttempts::integer
      )::text"""
        .query[String]
        .unique
        .map(parseIntOrZero)
    }

  def markBatchDispatchFailed(
      loadJson: String,
      errorText: String,
      maxAttempts: Int
  ): IO[Either[DatabaseError, Unit]] =
    runDb("coordinator.mark_batch_dispatch_failed") {
      sql"""SELECT coordinator.mark_batch_dispatch_failed(
        $loadJson::jsonb,
        $errorText::text,
        $maxAttempts::integer
      )::text"""
        .query[String]
        .unique
        .map(_ => ())
    }

  def releaseBatchDispatch(
      loadJson: String,
      errorText: String
  ): IO[Either[DatabaseError, Unit]] =
    runDb("coordinator.release_batch_dispatch") {
      sql"""SELECT coordinator.release_batch_dispatch(
        $loadJson::jsonb,
        $errorText::text
      )::text"""
        .query[String]
        .unique
        .map(_ => ())
    }

  def ensureCursor(streamName: String): IO[Either[DatabaseError, String]] =
    runDb("coordinator.ensure_cursor") {
      sql"""SELECT coordinator.ensure_cursor($streamName::text)::text"""
        .query[String]
        .unique
    }

  def ensureAllCursors(streamNames: List[String]): IO[Either[DatabaseError, String]] =
    runDb("coordinator.ensure_all_cursors") {
      streamNames.traverse { name =>
        sql"""SELECT coordinator.ensure_cursor($name::text)::text""".query[String].unique
      }.map(_.last)
    }

  def generateShadowAlerts(): IO[Either[DatabaseError, List[String]]] =
    runDb("coordinator.generate_shadow_alerts") {
      sql"""SELECT coordinator.generate_shadow_alerts()::text"""
        .query[String]
        .to[List]
    }

  private def runDb[A](operation: String)(fa: ConnectionIO[A]): IO[Either[DatabaseError, A]] =
    fa.transact(xa)
      .map(Right(_))
      .handleError { cause =>
        log.error("event=db_error operation={} error=\"{}\"", operation, sanitize(cause.getMessage))
        Left(DatabaseError.Retryable(operation, cause, cause.getMessage))
      }

  private def parseSyncLoad(json: String): SyncLoad =
    import io.circe.parser.decode as circeDecode
    circeDecode[SyncLoad](json) match
      case Right(load) => load
      case Left(err) =>
        log.error("event=parse_sync_load status=failed error=\"{}\"", sanitize(err.getMessage))
        throw err

  private def parseIntOrZero(s: String): Int =
    if s == null || s.isBlank then 0 else s.trim.toInt

  private def parseLongOrZero(s: String): Long =
    if s == null || s.isBlank then 0L else s.trim.toLong

  private def blankToNull(s: String): Option[String] =
    if s == null || s.isEmpty then None else Some(s)

  private def sanitize(msg: String): String =
    if msg == null then "" else msg.replace('\n', ' ').replace('\r', ' ')

object CoordinatorRepository:
  private val log = LoggerFactory.getLogger(getClass)
