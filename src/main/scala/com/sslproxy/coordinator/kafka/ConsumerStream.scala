package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
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
  private val sinkBatchSize = 500
  private val sinkBatchInterval = 15.seconds

  def run(
      cfg: KafkaCfg,
      sinkPipe: SinkPipe,
      dlqProducer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    val consumerSettings = ConsumerSettings[IO, String, String]
      .withBootstrapServers(cfg.bootstrapServers)
      .withGroupId(cfg.loadConsumer)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withMaxPollRecords(cfg.maxPollRecords)
      .withProperties(
        "allow.auto.create.topics" -> "false",
        "session.timeout.ms" -> "30000",
        "heartbeat.interval.ms" -> "3000"
      )

    val parallelism = 4
    Stream
      .resource(fs2.kafka.KafkaConsumer.resource(consumerSettings))
      .flatMap { consumer =>
        Stream.eval(consumer.subscribeTo(cfg.loadTopic)) >>
        consumer.partitionedStream
          .map { partitionStream =>
            partitionStream
              .evalMap { committable =>
                decodeWithDlq(cfg, dlqProducer, committable)
              }
              .groupWithin(sinkBatchSize, sinkBatchInterval)
              .evalMap { chunk =>
                dbSemaphore.permit.use { _ =>
                  val items = chunk.toList
                  val offsets = items.map(_._2)
                  val records = items.flatMap(_._1.toList)
                  if records.nonEmpty then
                    sinkPipe.processBatch(records.map(e => (e.table, e.ctx, e.row)))
                      .handleErrorWith { err =>
                        IO(log.error("event=sink_pipe_batch status=failed error=\"{}\"", sanitize(err.getMessage)))
                      }
                      .as(offsets)
                  else IO.pure(offsets)
                }
              }
              .flatMap(Stream.emits)
          }
          .parJoin(parallelism.max(1))
          .through(commitBatch)
      }

  private def decodeWithDlq(
      cfg: KafkaCfg,
      dlqProducer: KafkaProducer[IO, String, String],
      committable: CommittableConsumerRecord[IO, String, String]
  ): IO[(Option[KafkaEnvelope], CommittableOffset[IO])] =
    val record = committable.record
    val json = record.value

    IO.fromEither(KafkaEnvelope.decode(json))
      .flatMap { envelope =>
        IO(log.debug("event=consumer_record status=processing table={} origin={}",
          envelope.table, envelope.ctx.origin))
          .as((Some(envelope), committable.offset))
      }
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
        dlqProducer.produce(ProducerRecords.one(dlqRecord)).flatten.void.as((None, committable.offset))
      }

  private def commitBatch: fs2.Pipe[IO, CommittableOffset[IO], Unit] =
    _.groupWithin(commitBatchSize, commitInterval)
      .evalMap(CommittableOffsetBatch.fromFoldable(_).commit)

  private def sanitize(msg: String): String =
    if msg == null then "" else msg.replace('\n', ' ').replace('\r', ' ')
