package com.sslproxy.coordinator.dispatch

import cats.effect.IO
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.tidb.{OutboxFailureDisposition, OutboxRecord, TidbRepository}
import fs2.kafka.{KafkaProducer, ProducerRecord, ProducerRecords}
import org.slf4j.LoggerFactory

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
        IO(log.error(
          "event=outbox_claim status=db_error operation={} error=\"{}\"",
          error.operation,
          sanitize(error.message)
        )).as(false)
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
          IO(log.info(
            "event=outbox_publish status=published outbox_id={} topic={} message_key={} fence={}",
            record.outboxId,
            record.destinationTopic,
            record.messageKey,
            record.lease.fence
          )).as(true)
      case Right(false) =>
        IO(log.warn(
          "event=outbox_publish status=lease_lost_after_publish outbox_id={} fence={}",
          record.outboxId,
          record.lease.fence
        )).as(false)
      case Left(error) =>
        IO(log.error(
          "event=outbox_publish status=ack_failed outbox_id={} operation={} error=\"{}\"",
          record.outboxId,
          error.operation,
          sanitize(error.message)
        )).as(false)
    }

  private def fail(record: OutboxRecord, cause: Throwable): IO[Boolean] =
    val message = Option(cause.getMessage).getOrElse(cause.getClass.getSimpleName)
    repo.failOutbox(record, message, retryBaseSeconds, retryMaxSeconds).flatMap {
      case Right(disposition) =>
        val status = disposition match
          case OutboxFailureDisposition.RetryScheduled => "retry_scheduled"
          case OutboxFailureDisposition.Parked         => "parked"
        IO(log.warn(
          "event=outbox_publish status={} outbox_id={} attempt={} error=\"{}\"",
          status,
          record.outboxId,
          record.attemptCount,
          sanitize(message)
        )).as(false)
      case Left(error) =>
        IO(log.error(
          "event=outbox_publish status=fail_transition_failed outbox_id={} operation={} error=\"{}\"",
          record.outboxId,
          error.operation,
          sanitize(error.message)
        )).as(false)
    }

  private def sanitize(message: String): String =
    if message == null then "" else message.replace('\n', ' ').replace('\r', ' ')

object BatchDispatchService:
  private val log = LoggerFactory.getLogger(getClass)
