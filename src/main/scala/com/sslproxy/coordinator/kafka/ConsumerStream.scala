package com.sslproxy.coordinator.kafka

import cats.effect.IO
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.errors.DeadLetter
import com.sslproxy.coordinator.model.SystemContext
import com.sslproxy.coordinator.sink.SinkPipe
import fs2.Stream
import fs2.kafka.*
import org.slf4j.LoggerFactory

import scala.concurrent.duration.*

object ConsumerStream:
  private val log = LoggerFactory.getLogger(getClass)
  private val commitBatchSize = 500
  private val commitInterval = 15.seconds

  def run(
      cfg: KafkaCfg,
      sinkPipe: SinkPipe,
      dlqProducer: KafkaProducer[IO, String, String]
  ): Stream[IO, Unit] =
    val consumerSettings = ConsumerSettings[IO, String, String]
      .withBootstrapServers(cfg.bootstrapServers)
      .withGroupId(cfg.consumerGroup)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withMaxPollRecords(cfg.maxPollRecords)
      .withProperties(
        "allow.auto.create.topics" -> "false",
        "session.timeout.ms" -> "30000",
        "heartbeat.interval.ms" -> "3000"
      )

    Stream
      .resource(fs2.kafka.KafkaConsumer.resource(consumerSettings))
      .flatMap { consumer =>
        Stream.eval(consumer.subscribeTo(cfg.loadTopic)) >>
        consumer.partitionedStream
          .map { partitionStream =>
            partitionStream
              .evalMap { committable =>
                processRecord(cfg, sinkPipe, dlqProducer, committable)
              }
          }
          .parJoinUnbounded
          .through(commitBatch)
      }

  private def processRecord(
      cfg: KafkaCfg,
      sinkPipe: SinkPipe,
      dlqProducer: KafkaProducer[IO, String, String],
      committable: CommittableConsumerRecord[IO, String, String]
  ): IO[CommittableOffset[IO]] =
    val record = committable.record
    val json = record.value

    val result = for
      envelope <- IO.fromEither(KafkaEnvelope.decode(json))
      _        <- IO(log.debug("event=consumer_record status=processing table={} origin={}",
                     envelope.table, envelope.ctx.origin))
      _        <- fs2.Stream
                    .emit((envelope.table, envelope.ctx, envelope.row))
                    .through(sinkPipe.pipe)
                    .compile.drain
    yield ()

    result
      .handleErrorWith { err =>
        val dlqTopic = record.topic + cfg.dlqSuffix
        val dl = DeadLetter(
          table = record.topic,
          ctx = SystemContext("unknown", "unknown"),
          row = Map.empty,
          error = err.getMessage
        )
        log.error("event=consumer_record status=dlq topic={} offset={} error=\"{}\"",
          record.topic, committable.offset, sanitize(err.getMessage))

        val dlqRecord = ProducerRecord(dlqTopic, record.key, dl.toDlqJson)
        dlqProducer.produce(ProducerRecords.one(dlqRecord)).flatten.void
      }
      .as(committable.offset)

  private def commitBatch: fs2.Pipe[IO, CommittableOffset[IO], Unit] =
    _.groupWithin(commitBatchSize, commitInterval)
      .evalMap(CommittableOffsetBatch.fromFoldable(_).commit)

  private def sanitize(msg: String): String =
    if msg == null then "" else msg.replace('\n', ' ').replace('\r', ' ')
