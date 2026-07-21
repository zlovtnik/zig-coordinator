package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.cutover.VerifiedCutoverArtifact
import com.sslproxy.coordinator.tidb.TidbRepository
import fs2.Stream
import org.slf4j.LoggerFactory

object TidbResultStream:
  private val log = LoggerFactory.getLogger(getClass)

  def run(
      cfg: KafkaCfg,
      artifact: VerifiedCutoverArtifact,
      repo: TidbRepository,
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(cfg, cfg.resultConsumer, cfg.resultTopic, artifact) { locked =>
      for
        result <- IO.fromEither(KafkaComponents.deserializeResult(locked.record.value))
        _ <- dbSemaphore.permit.use { _ =>
          KafkaDatabaseResult.require(
            repo.recordResultWithEvidence(result, locked.metadata)
          )
        }
        _ <- IO(log.info(
          "event=tidb_result_consumer status=recorded batch_id={} result_status={} group={} partition={} offset={}",
          result.batchId,
          result.status,
          locked.metadata.consumerGroup,
          locked.metadata.partition,
          locked.metadata.offset
        ))
      yield ()
    }
