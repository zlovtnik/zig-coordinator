package com.sslproxy.coordinator.kafka

import cats.effect.{IO, Resource}
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.tidb.{TidbLoad, TidbResult}
import fs2.kafka.*
import io.circe.parser.decode as circeDecode
import io.circe.syntax.*
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.slf4j.LoggerFactory

import java.util.{Collections, Properties}
import scala.concurrent.duration.*

final class KafkaComponents(
    val producer: KafkaProducer[IO, String, String],
    val config: KafkaCfg
)

object KafkaComponents:
  private val log = LoggerFactory.getLogger(getClass)

  def resource(cfg: KafkaCfg): Resource[IO, KafkaComponents] =
    createProducer(cfg).map(producer => KafkaComponents(producer, cfg))

  /** Create the topic on the broker if it does not already exist. */
  private def ensureTopicExists(
      bootstrapServers: String,
      topic: String,
      numPartitions: Int = 3,
      replicationFactor: Short = 1
  ): IO[Unit] =
    IO.blocking:
      val props = new Properties()
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      val admin = Admin.create(props)
      try
        val existing = admin.listTopics().names().get()
        if !existing.contains(topic) then
          val newTopic = new NewTopic(topic, numPartitions, replicationFactor)
          admin.createTopics(Collections.singletonList(newTopic)).all().get()
          log.info("event=topic_provision status=created topic={} partitions={} replication={}",
            topic, numPartitions, replicationFactor)
        else
          log.debug("event=topic_provision status=exists topic={}", topic)
      finally
        admin.close()

  /** Ensure the topic exists on the broker, then block until it is ready for
    * consuming.  Uses a temporary consumer to probe `partitionsFor` with retry.
    */
  def waitForTopic(
      bootstrapServers: String,
      topic: String,
      timeout: FiniteDuration = 30.seconds,
      retryInterval: FiniteDuration = 2.seconds
  ): IO[Unit] =
    val settings = ConsumerSettings[IO, String, String]
      .withBootstrapServers(bootstrapServers)
      .withGroupId(s"preflight-${topic}-${System.currentTimeMillis()}")
      .withProperties("allow.auto.create.topics" -> "false")

    def loop(deadline: FiniteDuration): IO[Unit] =
      KafkaConsumer.resource(settings).use { consumer =>
        consumer.partitionsFor(topic).attempt.flatMap {
          case Right(partitions) if partitions.isEmpty =>
            IO(log.warn("event=topic_preflight status=empty topic={}", topic)) *>
              retryOrTimeout(deadline)(loop(deadline))
          case Right(_) =>
            IO(log.info("event=topic_preflight status=ready topic={}", topic))
          case Left(ex) if ex.isInstanceOf[org.apache.kafka.common.errors.UnknownTopicOrPartitionException] =>
            IO(log.warn("event=topic_preflight status=waiting topic={} error=\"{}\"",
              topic, ex.getMessage)) *>
              retryOrTimeout(deadline)(loop(deadline))
          case Left(ex) =>
            IO.raiseError(ex)
        }
      }

    def retryOrTimeout(deadline: FiniteDuration)(retry: => IO[Unit]): IO[Unit] =
      IO.monotonic.flatMap { now =>
        if now >= deadline then
          IO.raiseError(new IllegalStateException(
            s"topic $topic did not become available within $timeout"))
        else
          IO.sleep(retryInterval) *> retry
      }

    ensureTopicExists(bootstrapServers, topic) *>
      IO.monotonic.flatMap { start => loop(start + timeout) }

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
