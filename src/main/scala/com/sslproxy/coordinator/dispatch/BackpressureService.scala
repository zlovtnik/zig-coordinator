package com.sslproxy.coordinator.dispatch

import cats.effect.IO
import cats.effect.kernel.Ref
import com.sslproxy.coordinator.config.BackpressureConfig
import com.sslproxy.coordinator.domain.DatabaseError
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import org.slf4j.LoggerFactory

class BackpressureService(
    cfg: BackpressureConfig,
    ingestBatchSize: Int,
    pendingLedgerCount: IO[Either[DatabaseError, Long]],
    metrics: CoordinatorMetrics
):
  import BackpressureService.log

  private val consumerSuspended: Ref[IO, Boolean] = Ref.unsafe[IO, Boolean](false)

  def budget: Long =
    ingestBatchSize.toLong * cfg.budgetMultiplier

  def recoveryThreshold: Long =
    budget / 2

  def isConsumerSuspended: IO[Boolean] =
    consumerSuspended.get

  def checkAndAct: IO[Long] =
    pendingLedgerCount.flatMap {
      case Left(err) =>
        IO(log.warn("event=backpressure status=pending_count_failed operation={} error=\"{}\"",
          err.operation, sanitize(err.message))) *>
          consumerSuspended.get.flatMap { suspended =>
            IO(metrics.recordBackpressureActive(suspended))
              .as(if suspended then budget else 0L)
          }

      case Right(count) =>
        IO(metrics.recordPendingLedgerCount(count)) *>
          evaluate(count).as(count)
    }

  private def evaluate(pendingCount: Long): IO[Unit] =
    val b = budget
    val rt = recoveryThreshold

    consumerSuspended.get.flatMap { suspended =>
      if pendingCount >= b then
        if !suspended then
          consumerSuspended.set(true) *>
            IO(log.info("event=backpressure status=throttled pending_count={} budget={} multiplier={}",
              pendingCount, b, cfg.budgetMultiplier))
        else IO.unit
      else if pendingCount <= rt && suspended then
        consumerSuspended.set(false) *>
          IO(log.info("event=backpressure status=recovered pending_count={} recovery_threshold={}",
            pendingCount, rt))
      else IO.unit
    } *> consumerSuspended.get.flatMap(s => IO(metrics.recordBackpressureActive(s)))

  private def sanitize(msg: String): String =
    if msg == null then "" else msg.replace('\n', ' ').replace('\r', ' ')

object BackpressureService:
  private val log = LoggerFactory.getLogger(getClass)
