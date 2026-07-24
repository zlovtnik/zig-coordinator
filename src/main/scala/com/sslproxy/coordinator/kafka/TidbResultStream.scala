package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.cutover.{CutoffKey, VerifiedCutoverArtifact}
import com.sslproxy.coordinator.tidb.TidbRepository
import fs2.Stream
import com.sslproxy.coordinator.observability.StructuredLogger

object TidbResultStream:
  private val log = StructuredLogger(getClass)

  def run(
      cfg: KafkaCfg,
      artifact: VerifiedCutoverArtifact,
      repo: TidbRepository,
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(cfg, cfg.resultConsumer, cfg.resultTopic, artifact,
      repo.loadConsumerOffsets(cfg.resultConsumer, cfg.resultTopic).map(_.fold(_ => Set.empty[CutoffKey], identity))
    ) { locked =>
      for
        result <- IO.fromEither(KafkaComponents.deserializeResult(locked.record.value))
        _ <- dbSemaphore.permit.use { _ =>
          KafkaDatabaseResult.require(
            repo.recordResultWithEvidence(result, locked.metadata)
          )
        }
        _ <- IO(log.info("tidb_result_consumer", "status" -> "recorded",
          "batch_id" -> result.batchId, "result_status" -> result.status,
          "group" -> locked.metadata.consumerGroup,
          "partition" -> locked.metadata.partition.toString,
          "offset" -> locked.metadata.offset.toString))
      yield ()
    }
