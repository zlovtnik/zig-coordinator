package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.cutover.VerifiedCutoverArtifact
import com.sslproxy.coordinator.domain.ScanRequestRecord
import com.sslproxy.coordinator.tidb.TidbRepository
import fs2.Stream
import org.slf4j.LoggerFactory

object ScanRequestStream:
  private val log = LoggerFactory.getLogger(getClass)

  def run(
      cfg: KafkaCfg,
      artifact: VerifiedCutoverArtifact,
      repo: TidbRepository,
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(cfg, cfg.scanConsumer, cfg.scanTopic, artifact) { locked =>
      for
        request <- IO.fromEither(ScanRequestRecord.decodeWire(locked.record.value))
        decision <- dbSemaphore.permit.use { _ =>
          KafkaDatabaseResult.require(
            repo.recordScanRequestWithEvidence(request, locked.metadata)
          )
        }
        _ <- IO(log.info(
          "event=scan_request_consumer status={} stream_name={} group={} partition={} offset={}",
          decision.disposition.databaseValue,
          request.streamName,
          locked.metadata.consumerGroup,
          locked.metadata.partition,
          locked.metadata.offset
        ))
      yield ()
    }
