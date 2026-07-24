package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.cutover.{CutoffKey, VerifiedCutoverArtifact}
import com.sslproxy.coordinator.domain.ScanRequestRecord
import com.sslproxy.coordinator.tidb.TidbRepository
import fs2.Stream
import com.sslproxy.coordinator.observability.StructuredLogger

object ScanRequestStream:
  private val log = StructuredLogger(getClass)

  def run(
      cfg: KafkaCfg,
      artifact: VerifiedCutoverArtifact,
      repo: TidbRepository,
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(cfg, cfg.scanConsumer, cfg.scanTopic, artifact,
      repo.loadConsumerOffsets(cfg.scanConsumer, cfg.scanTopic).map(_.fold(_ => Set.empty[CutoffKey], identity))
    ) { locked =>
      for
        request <- IO.fromEither(ScanRequestRecord.decodeWire(locked.record.value))
        decision <- dbSemaphore.permit.use { _ =>
          KafkaDatabaseResult.require(
            repo.recordScanRequestWithEvidence(request, locked.metadata)
          )
        }
        _ <- IO(log.info("scan_request_consumer",
          "status" -> decision.disposition.databaseValue,
          "stream_name" -> request.streamName,
          "group" -> locked.metadata.consumerGroup,
          "partition" -> locked.metadata.partition.toString,
          "offset" -> locked.metadata.offset.toString))
      yield ()
    }
