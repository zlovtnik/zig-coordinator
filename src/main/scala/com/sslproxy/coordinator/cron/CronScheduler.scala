package com.sslproxy.coordinator.cron

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all.*
import com.sslproxy.coordinator.config.{CronConfig, IngestConfig}
import com.sslproxy.coordinator.dispatch.{BackpressureService, BatchDispatchService}
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.postgres.CoordinatorRepository
import com.sslproxy.coordinator.sink.SchemaIntrospector
import fs2.Stream
import org.slf4j.LoggerFactory

import scala.concurrent.duration.*

class CronScheduler(
    cfg: CronConfig,
    ingestConfig: IngestConfig,
    repo: CoordinatorRepository,
    backpressureService: BackpressureService,
    batchDispatchService: BatchDispatchService,
    metrics: CoordinatorMetrics,
    schemaIntrospector: SchemaIntrospector
):
  import CronScheduler.log

  private val loopCounter: Ref[IO, Long] = Ref.unsafe[IO, Long](0L)
  private val lastShadowAuditMs: Ref[IO, Long] = Ref.unsafe[IO, Long](0L)

  private val knownTables: List[String] = List(
    "proxy_events",
    "proxy_blocked_host_rollups",
    "proxy_payload_audit",
    "wireless_sensors",
    "wireless_audit_frames",
    "wireless_bandwidth_windows",
    "wireless_alerts",
    "wireless_alerts_ledger",
    "wireless_client_inventory",
    "wireless_probe_requests"
  )

  val schemaRefresher: Stream[IO, Unit] =
    schemaIntrospector.startRefresher(knownTables)

  val mainLoop: Stream[IO, Unit] =
    Stream
      .awakeEvery[IO](cfg.idleSleepMs.millis)
      .evalMap { _ =>
        val tick: IO[Unit] = for
          _ <- adaptivePull()
          _ <- backpressureService.checkAndAct.void
          _ <- processIngest()
          _ <- recoverStaleBatches()
          _ <- dispatchBatches()
          _ <- shadowAudit()
          _ <- metrics.heartbeat()
          _ <- IO(metrics.incrementLoopCounter())
          _ <- loopCounter.update(_ + 1)
        yield ()

        tick.handleErrorWith { err =>
          IO(log.error("event=cron_tick status=failed error=\"{}\"", sanitize(err.getMessage))) *>
            IO(metrics.recordTickFailure())
        }
      }

  private def adaptivePull(): IO[Unit] =
    IO.unit // Stub — dynamic maxPollRecords not yet ported; backpressure provides primary flow control

  private def processIngest(): IO[Unit] =
    val budget = cfg.ingestBatchSize.toLong * 2

    repo.pendingLedgerCount().flatMap {
      case Left(err) =>
        IO(log.warn("event=ingest_ledger status=pending_count_failed operation={} error=\"{}\"",
          err.operation, sanitize(err.message))) *>
          IO(metrics.recordIngestInvocation(false))

      case Right(pendingCount) =>
        val logPending = IO(log.info("event=ingest_ledger status=pending count={}", pendingCount))
        val throttleCheck = if pendingCount >= budget then
          IO(log.info("event=backpressure status=throttled pending_count={} budget={} ingest_batch_size={}",
            pendingCount, budget, cfg.ingestBatchSize))
        else IO.unit

        logPending *> throttleCheck *>
          repo.processIngestLedger(
            ingestConfig.streamNames,
            ingestConfig.loadStreamNames,
            cfg.scanMaxAttempts,
            cfg.scanRetryBackoffSeconds,
            cfg.ingestBatchSize
          ).flatMap {
            case Left(err) =>
              IO(log.error("event=ingest_ledger status=failed operation={} error=\"{}\"",
                err.operation, sanitize(err.message))) *>
                IO(metrics.recordIngestInvocation(false))

            case Right(processed) =>
              IO(metrics.recordIngestInvocation(true)) *>
                IO(metrics.recordIngestProcessed(processed)) *>
                IO.whenA(processed > 0)(
                  IO(log.info("event=ingest_ledger status=processed count={}", processed))
                )
          }
    }

  private def recoverStaleBatches(): IO[Unit] =
    repo.recoverStaleDispatchedBatches(
      ingestConfig.loadStreamNames,
      cfg.batchDispatchLeaseSeconds,
      cfg.batchMaxAttempts
    ).flatMap {
      case Left(err) =>
        IO(log.error("event=stale_batch_recovery status=failed operation={} error=\"{}\"",
          err.operation, sanitize(err.message)))
      case Right(count) =>
        IO.whenA(count > 0)(
          IO(log.info("event=stale_batch_recovery status=recovered count={}", count))
        )
    }

  private def dispatchBatches(): IO[Unit] =
    (1 to cfg.dispatchBatchSize).toList.traverse_ { _ =>
      batchDispatchService.dispatchNext().flatMap {
        case true  => IO.unit
        case false => IO.unit
      }
    }

  private def shadowAudit(): IO[Unit] =
    val intervalMs = 10_000L

    lastShadowAuditMs.get.flatMap { lastMs =>
      val now = System.currentTimeMillis()
      if now - lastMs < intervalMs then IO.unit
      else
        repo.generateShadowAlerts().flatMap {
          case Left(err) =>
            IO(log.error("event=shadow_audit status=failed operation={} error=\"{}\"",
              err.operation, sanitize(err.message))) *>
              lastShadowAuditMs.set(now)

          case Right(alerts) =>
            IO.whenA(alerts.nonEmpty)(
              IO(log.info("event=shadow_audit status=alerts_generated count={} result=\"{}\"",
                alerts.size, alerts))
            ) *> lastShadowAuditMs.set(now)
        }
    }

  private def sanitize(msg: String): String =
    if msg == null then "" else msg.replace('\n', ' ').replace('\r', ' ')

object CronScheduler:
  private val log = LoggerFactory.getLogger(getClass)
