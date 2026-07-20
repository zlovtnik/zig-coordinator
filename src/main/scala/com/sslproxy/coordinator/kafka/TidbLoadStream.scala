package com.sslproxy.coordinator.kafka

import cats.effect.IO
import com.sslproxy.coordinator.config.KafkaConfig
import com.sslproxy.coordinator.tidb.{TidbLoadHandler, TidbResult}
import fs2.Stream
import fs2.kafka.*
import io.circe.syntax.*
import org.slf4j.LoggerFactory

import scala.concurrent.duration.*

object TidbLoadStream:
  private val log = LoggerFactory.getLogger(getClass)

  def run(
      components: KafkaComponents,
      handler: TidbLoadHandler,
  ): Stream[IO, Unit] =
    val cfg = components.config
    components.consumer
      .partitionedStream
      .map { partitionStream =>
        partitionStream
          .filter(_.record.topic == cfg.loadTopic)
          .evalMap { committable =>
            val record = committable.record
            val json = record.value

            (for
              load <- IO.fromEither(KafkaComponents.deserializeLoad(json))
              _    <- IO(log.info("event=tidb_load_consumer status=processing batch_id={} stream_name={}",
                         load.batchId, load.streamName))
              result <- handler.handle(load)
              _      <- produceResult(components, cfg, result)
              _      <- IO(log.info("event=tidb_load_consumer status=completed batch_id={} status={} row_count={}",
                         load.batchId, result.status, result.rowCount))
            yield ()).handleErrorWith { err =>
              IO(log.error("event=tidb_load_consumer status=failed error=\"{}\"", err.getMessage)) *>
              produceDlq(components, cfg, record, err)
            }.as(committable.offset)
          }
      }
      .parJoin(1)
      .through(commitBatch(cfg.pollTimeoutMs))

  private def produceResult(
      components: KafkaComponents,
      cfg: KafkaConfig,
      result: TidbResult
  ): IO[Unit] =
    val record = ProducerRecord(cfg.resultTopic, result.jobId, result.asJson.noSpaces)
    components.producer.produce(ProducerRecords.one(record)).flatten.void

  private def produceDlq(
      components: KafkaComponents,
      cfg: KafkaConfig,
      record: ConsumerRecord[String, String],
      err: Throwable
  ): IO[Unit] =
    val dlqTopic = cfg.loadTopic + cfg.dlqSuffix
    val dlqValue = s"""{"original":${record.value},"error":"${sanitize(err.getMessage)}"}"""
    val dlqRecord = ProducerRecord(dlqTopic, record.key, dlqValue)
    components.producer.produce(ProducerRecords.one(dlqRecord)).flatten.void

  private def commitBatch(timeoutMs: Long): fs2.Pipe[IO, CommittableOffset[IO], Unit] =
    _.groupWithin(50, timeoutMs.millis)
      .evalMap(CommittableOffsetBatch.fromFoldable(_).commit)

  private def sanitize(msg: String): String =
    if msg == null then "" else msg.replace('\n', ' ').replace('\r', ' ')
