package com.sslproxy.coordinator.config

import com.sslproxy.coordinator.tidb.TidbConfig

final case class CoordinatorConfig(
    tidb: TidbConfig,
    kafka: KafkaConfig,
    syncOutboxDir: String,
    httpPort: Int
)

object CoordinatorConfig:
  def fromEnv: CoordinatorConfig =
    CoordinatorConfig(
      tidb = TidbConfig.fromEnv,
      kafka = KafkaConfig.fromEnv,
      syncOutboxDir = sys.env.getOrElse("SYNC_OUTBOX_DIR", "/var/lib/sync/outbox"),
      httpPort = sys.env.get("COORDINATOR_HTTP_PORT").flatMap(v => scala.util.Try(v.toInt).toOption).getOrElse(8081)
    )

final case class KafkaConfig(
    bootstrapServers: String,
    loadTopic: String,
    resultTopic: String,
    dlqSuffix: String,
    consumerGroup: String,
    maxPollRecords: Int,
    pollTimeoutMs: Long
)

object KafkaConfig:
  val DefaultBootstrapServers: String = "localhost:9092"
  val DefaultLoadTopic: String = "sync.oracle.load"
  val DefaultResultTopic: String = "sync.oracle.result"
  val DefaultDlqSuffix: String = ".dlq"
  val DefaultConsumerGroup: String = "tidb-worker-load"
  val DefaultMaxPollRecords: Int = 50
  val DefaultPollTimeoutMs: Long = 3000L

  def fromEnv: KafkaConfig =
    KafkaConfig(
      bootstrapServers = sys.env.getOrElse("SYNC_REDPANDA_BOOTSTRAP_SERVERS", DefaultBootstrapServers),
      loadTopic = sys.env.getOrElse("SYNC_LOAD_TOPIC", DefaultLoadTopic),
      resultTopic = sys.env.getOrElse("SYNC_RESULT_TOPIC", DefaultResultTopic),
      dlqSuffix = sys.env.getOrElse("SYNC_DLQ_SUFFIX", DefaultDlqSuffix),
      consumerGroup = sys.env.getOrElse("TIDB_LOAD_CONSUMER_GROUP", DefaultConsumerGroup),
      maxPollRecords = sys.env.get("TIDB_MAX_POLL_RECORDS").flatMap(v => scala.util.Try(v.toInt).toOption).getOrElse(DefaultMaxPollRecords),
      pollTimeoutMs = sys.env.get("TIDB_POLL_TIMEOUT_MS").flatMap(v => scala.util.Try(v.toLong).toOption).getOrElse(DefaultPollTimeoutMs)
    )
