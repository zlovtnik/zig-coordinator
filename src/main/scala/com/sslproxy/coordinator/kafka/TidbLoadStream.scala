package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.cutover.VerifiedCutoverArtifact
import com.sslproxy.coordinator.tidb.{TidbLoadHandler, TidbRepository}
import fs2.Stream
import org.slf4j.LoggerFactory

object TidbLoadStream:
  private val log = LoggerFactory.getLogger(getClass)

  def run(
      cfg: KafkaCfg,
      artifact: VerifiedCutoverArtifact,
      repo: TidbRepository,
      handler: TidbLoadHandler,
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(cfg, cfg.loadConsumer, cfg.loadTopic, artifact) { locked =>
      dbSemaphore.permit.use { _ =>
        for
          load <- IO.fromEither(KafkaComponents.deserializeLoad(locked.record.value))
          _ <- IO(log.info(
            "event=tidb_load_consumer status=processing batch_id={} stream_name={} group={} partition={} offset={}",
            load.batchId,
            load.streamName,
            locked.metadata.consumerGroup,
            locked.metadata.partition,
            locked.metadata.offset
          ))
          result <- handler.handle(load)
          _ <- KafkaDatabaseResult.require(
            repo.recordLoadResultWithEvidence(load, result, locked.metadata)
          )
          _ <- IO(log.info(
            "event=tidb_load_consumer status=durable batch_id={} result_status={} row_count={}",
            load.batchId,
            result.status,
            result.rowCount
          ))
        yield ()
      }
    }
