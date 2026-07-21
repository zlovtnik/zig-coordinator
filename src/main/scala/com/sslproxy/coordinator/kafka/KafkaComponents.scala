package com.sslproxy.coordinator.kafka

import cats.effect.{IO, Resource}
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.tidb.{TidbLoad, TidbResult}
import fs2.kafka.*
import io.circe.parser.decode as circeDecode
import io.circe.syntax.*

final class KafkaComponents(
    val producer: KafkaProducer[IO, String, String],
    val config: KafkaCfg
)

object KafkaComponents:

  def resource(cfg: KafkaCfg): Resource[IO, KafkaComponents] =
    createProducer(cfg).map(producer => KafkaComponents(producer, cfg))

  /** Every processor gets its own KafkaConsumer resource. There is deliberately
    * no shared consumer here: a subscription and group identity are part of the
    * processor's durable contract.
    */
  private[kafka] def consumerSettings(
      cfg: KafkaCfg,
      groupId: String
  ): ConsumerSettings[IO, String, String] =
    ConsumerSettings[IO, String, String]
      .withBootstrapServers(cfg.bootstrapServers)
      .withGroupId(groupId)
      .withAutoOffsetReset(AutoOffsetReset.None)
      .withEnableAutoCommit(false)
      .withIsolationLevel(IsolationLevel.ReadCommitted)
      .withMaxPollRecords(cfg.maxPollRecords)
      .withProperties(
        "allow.auto.create.topics" -> "false",
        "session.timeout.ms" -> "30000",
        "heartbeat.interval.ms" -> "3000"
      )

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

  def deserializeResult(json: String): Either[Throwable, TidbResult] =
    circeDecode[TidbResult](json)

  def serializeResult(result: TidbResult): String =
    result.asJson.noSpaces
