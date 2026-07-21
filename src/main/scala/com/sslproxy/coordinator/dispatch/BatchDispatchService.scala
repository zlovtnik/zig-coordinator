package com.sslproxy.coordinator.dispatch

import cats.effect.IO
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.domain.SyncLoad
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.tidb.TidbRepository
import fs2.kafka.*
import io.circe.syntax.*
import org.slf4j.LoggerFactory

class BatchDispatchService(
    repo: TidbRepository,
    producer: KafkaProducer[IO, String, String],
    cfg: KafkaCfg,
    metrics: CoordinatorMetrics,
    loadStreamNames: List[String],
    batchMaxAttempts: Int
):
  import BatchDispatchService.log

  def dispatchNext(): IO[Boolean] =
    repo.getNextBatch(loadStreamNames).flatMap {
      case Left(err) =>
        log.error("event=batch_dispatch status=db_error operation={} error=\"{}\"",
          err.operation, sanitize(err.message))
        IO.pure(false)

      case Right(None) =>
        IO.pure(false)

      case Right(Some(load)) =>
        dispatchBatch(load)
    }

  private def dispatchBatch(load: SyncLoad): IO[Boolean] =
    validateLoad(load) match
      case Left(msg) =>
        log.error("event=batch_dispatch status=validation_failed batch_id={} error=\"{}\"",
          load.batchId, sanitize(msg))
        markFailed(load.asJson.noSpaces, msg) *> IO.pure(false)

      case Right(()) =>
        val dispatchJson = load.asJson.noSpaces
        log.info("event=batch_dispatch status=selected batch_id={} stream_name={} attempt={}",
          load.batchId, load.streamName, load.attempt)

        val record = ProducerRecord(cfg.loadTopic, load.jobId, dispatchJson)
        producer.produce(ProducerRecords.one(record)).flatten.attempt.flatMap {
          case Right(_) =>
            log.info("event=batch_dispatch status=published batch_id={} topic={}",
              load.batchId, cfg.loadTopic)
            IO(metrics.recordBatchDispatched()) *> IO.pure(true)

          case Left(publishErr) =>
            log.error("event=batch_dispatch status=publish_failed batch_id={} error=\"{}\"",
              load.batchId, sanitize(publishErr.getMessage))
            handlePublishFailure(dispatchJson, load, publishErr) *> IO.pure(false)
        }

  private def validateLoad(load: SyncLoad): Either[String, Unit] =
    if load == null then Left("load payload must not be null")
    else if load.jobId.isBlank then Left("job_id must not be empty")
    else if load.batchId.isBlank then Left("batch_id must not be empty")
    else if load.streamName.isBlank then Left("stream_name must not be empty")
    else if load.payloadRef.isBlank then Left("payload_ref must not be empty")
    else Right(())

  private def markFailed(batchJson: String, errorText: String): IO[Unit] =
    repo.markBatchDispatchFailed(batchJson, errorText, batchMaxAttempts).flatMap {
      case Left(err) =>
        log.error("event=batch_dispatch status=mark_failed_error operation={} error=\"{}\"",
          err.operation, sanitize(err.message))
        releaseBatch(batchJson, errorText)
      case Right(()) =>
        IO(log.info("event=batch_dispatch status=marked_failed"))
    }

  private def handlePublishFailure(batchJson: String, load: SyncLoad, err: Throwable): IO[Unit] =
    repo.markBatchDispatchFailed(batchJson, err.getMessage, batchMaxAttempts).flatMap {
      case Left(dbErr) =>
        log.error("event=batch_dispatch status=mark_failed_error batch_id={} operation={} error=\"{}\"",
          load.batchId, dbErr.operation, sanitize(dbErr.message))
        releaseBatch(batchJson, load, err.getMessage)
      case Right(()) =>
        IO(log.info("event=batch_dispatch status=marked_failed batch_id={}", load.batchId))
    }

  private def releaseBatch(batchJson: String, errorText: String): IO[Unit] =
    repo.releaseBatchDispatch(batchJson, errorText).flatMap {
      case Left(err) =>
        IO(log.error("event=batch_dispatch status=release_failed operation={} error=\"{}\"",
          err.operation, sanitize(err.message)))
      case Right(()) =>
        IO(log.info("event=batch_dispatch status=released"))
    }

  private def releaseBatch(batchJson: String, load: SyncLoad, errorText: String): IO[Unit] =
    repo.releaseBatchDispatch(batchJson, errorText).flatMap {
      case Left(err) =>
        IO(log.error("event=batch_dispatch status=release_failed batch_id={} operation={} error=\"{}\"",
          load.batchId, err.operation, sanitize(err.message)))
      case Right(()) =>
        IO(log.info("event=batch_dispatch status=released batch_id={}", load.batchId))
    }

  private def sanitize(msg: String): String =
    if msg == null then "" else msg.replace('\n', ' ').replace('\r', ' ')

object BatchDispatchService:
  private val log = LoggerFactory.getLogger(getClass)
