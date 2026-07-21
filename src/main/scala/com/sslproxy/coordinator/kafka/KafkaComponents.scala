package com.sslproxy.coordinator.kafka

import cats.effect.{IO, Resource}
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.tidb.{TidbLoad, TidbResult}
import fs2.kafka.*
import io.circe.parser.decode as circeDecode
import io.circe.syntax.*

final class KafkaComponents(
    val consumer: KafkaConsumer[IO, String, String],
    val producer: KafkaProducer[IO, String, String],
    val config: KafkaCfg
)

object KafkaComponents:

  def resource(cfg: KafkaCfg): Resource[IO, KafkaComponents] =
    for
      consumer <- createConsumer(cfg)
      producer <- createProducer(cfg)
    yield KafkaComponents(consumer, producer, cfg)

  private def createConsumer(cfg: KafkaCfg): Resource[IO, KafkaConsumer[IO, String, String]] =
    val consumerSettings = ConsumerSettings[IO, String, String]
      .withBootstrapServers(cfg.bootstrapServers)
      .withGroupId(cfg.loadConsumer + "-tidb-load")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withMaxPollRecords(cfg.maxPollRecords)
      .withProperties(
        "allow.auto.create.topics" -> "false",
        "session.timeout.ms" -> "30000",
        "heartbeat.interval.ms" -> "3000"
      )

    fs2.kafka.KafkaConsumer.resource(consumerSettings)

  private def createProducer(cfg: KafkaCfg): Resource[IO, KafkaProducer[IO, String, String]] =
    val producerSettings = ProducerSettings[IO, String, String]
      .withBootstrapServers(cfg.bootstrapServers)
      .withProperties(
        "allow.auto.create.topics" -> "false",
        "enable.idempotence" -> "true",
        "acks" -> "all",
        "retries" -> "3"
      )

    fs2.kafka.KafkaProducer.resource(producerSettings)

  def deserializeLoad(json: String): Either[Throwable, TidbLoad] =
    circeDecode[TidbLoad](json)

  def serializeResult(result: TidbResult): String =
    result.asJson.noSpaces
