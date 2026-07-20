package com.sslproxy.coordinator.cron

import cats.effect.IO
import cats.effect.kernel.Ref
import com.sslproxy.coordinator.config.CronConfig
import com.sslproxy.coordinator.sink.SchemaIntrospector
import doobie.*
import doobie.implicits.*
import fs2.Stream
import org.slf4j.LoggerFactory

import scala.concurrent.duration.*

class CronScheduler(
    cfg: CronConfig,
    xa: Transactor[IO],
    schemaIntrospector: SchemaIntrospector
):
  import CronScheduler.log

  private val loopCounter: Ref[IO, Long] = Ref.unsafe[IO, Long](0L)

  val mainLoop: Stream[IO, Unit] =
    Stream
      .awakeEvery[IO](cfg.idleSleepMs.millis)
      .evalMap { _ =>
        for
          _ <- heartbeat()
          _ <- processIngest()
          _ <- recoverStaleBatches()
          _ <- dispatchBatches()
          _ <- shadowAudit()
          _ <- loopCounter.update(_ + 1)
        yield ()
      }

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
  )

  private def processIngest(): IO[Unit] =
    sql"""SELECT COUNT(*) FROM proxy_events WHERE 1=1"""
      .query[Long]
      .unique
      .transact(xa)
      .flatMap { count =>
        IO(log.trace("event=cron_ingest status=ok pending_count={}", count))
      }
      .handleErrorWith { err =>
        IO(log.warn("event=cron_ingest status=failed error=\"{}\"", sanitize(err.getMessage)))
      }

  private def recoverStaleBatches(): IO[Unit] =
    IO.unit

  private def dispatchBatches(): IO[Unit] =
    IO.unit

  private def shadowAudit(): IO[Unit] =
    IO.unit

  private def heartbeat(): IO[Unit] =
    loopCounter.get.flatMap { count =>
      IO(log.info("event=cron_heartbeat status=running loop_count={}", count))
    }

  private def sanitize(msg: String): String =
    if msg == null then "" else msg.replace('\n', ' ').replace('\r', ' ')

object CronScheduler:
  private val log = LoggerFactory.getLogger(getClass)
