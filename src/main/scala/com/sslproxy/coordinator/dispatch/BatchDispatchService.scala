package com.sslproxy.coordinator.dispatch

import cats.effect.IO
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.tidb.{OutboxFailureDisposition, OutboxRecord, TidbRepository}
import fs2.kafka.{KafkaProducer, ProducerRecord, ProducerRecords}
import com.sslproxy.coordinator.observability.StructuredLogger

/** Publishes the transactional TiDB outbox. A broker acknowledgement followed
  * by a process crash can produce the same stable message key again; the
  * receiving transaction is therefore required to deduplicate that key.
  */
final class BatchDispatchService(
    repo: TidbRepository,
    producer: KafkaProducer[IO, String, String],
    metrics: CoordinatorMetrics,
    ownerId: String,
    destinationTopics: List[String],
    leaseSeconds: Int,
    retryBaseSeconds: Int,
    retryMaxSeconds: Int
):
  import BatchDispatchService.log

  def dispatchNext(): IO[Boolean] =
    repo.claimOutbox(ownerId, destinationTopics, leaseSeconds).flatMap {
      case Left(error) =>
        IO(log.error("outbox_claim", "status" -> "db_error",
          "operation" -> error.operation, "error" -> error.message)).as(false)
      case Right(None) => IO.pure(false)
      case Right(Some(record)) => publish(record)
    }

  private def publish(record: OutboxRecord): IO[Boolean] =
    val brokerRecord = ProducerRecord(
      record.destinationTopic,
      record.messageKey,
      record.payload
    )

    producer.produce(ProducerRecords.one(brokerRecord)).flatten.attempt.flatMap {
      case Right(_) => acknowledge(record)
      case Left(error) => fail(record, error)
    }

  private def acknowledge(record: OutboxRecord): IO[Boolean] =
    repo.acknowledgeOutbox(record).flatMap {
      case Right(true) =>
        IO(metrics.recordBatchDispatched()) *>
          IO(log.info("outbox_publish", "status" -> "published",
            "outbox_id" -> record.outboxId, "topic" -> record.destinationTopic,
            "message_key" -> record.messageKey, "fence" -> record.lease.fence.toString)).as(true)
      case Right(false) =>
        IO(log.warn("outbox_publish", "status" -> "lease_lost_after_publish",
          "outbox_id" -> record.outboxId, "fence" -> record.lease.fence.toString)).as(false)
      case Left(error) =>
        IO(log.error("outbox_publish", "status" -> "ack_failed",
          "outbox_id" -> record.outboxId, "operation" -> error.operation,
          "error" -> error.message)).as(false)
    }

  private def fail(record: OutboxRecord, cause: Throwable): IO[Boolean] =
    val message = Option(cause.getMessage).getOrElse(cause.getClass.getSimpleName)
    repo.failOutbox(record, message, retryBaseSeconds, retryMaxSeconds).flatMap {
      case Right(disposition) =>
        val status = disposition match
          case OutboxFailureDisposition.RetryScheduled => "retry_scheduled"
          case OutboxFailureDisposition.Parked         => "parked"
        IO(log.warn("outbox_publish", "status" -> status,
          "outbox_id" -> record.outboxId, "attempt" -> record.attemptCount.toString,
          "error" -> message)).as(false)
      case Left(error) =>
        IO(log.error("outbox_publish", "status" -> "fail_transition_failed",
          "outbox_id" -> record.outboxId, "operation" -> error.operation,
          "error" -> error.message)).as(false)
    }

object BatchDispatchService:
  private val log = StructuredLogger(getClass)
