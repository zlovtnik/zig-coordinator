package com.sslproxy.coordinator.config

import pureconfig.ConfigReader

final case class AppConfig(
    tidb: TiDbConfig,
    kafka: KafkaCfg,
    cron: CronConfig,
    ingest: IngestConfig,
    backpressure: BackpressureConfig,
    systemRegistry: SystemRegistryConfig,
    http: HttpConfig,
    sync: SyncConfig,
    wireless: WirelessConfig
) derives ConfigReader

final case class TiDbConfig(
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String,
    poolSize: Int,
    connectionTimeoutMs: Long,
    statementTimeoutSecs: Int,
    enabled: Boolean,
    warnOnly: Boolean,
    sslMode: String = "VERIFY_IDENTITY"
) derives ConfigReader

final case class KafkaCfg(
    bootstrapServers: String,
    loadTopic: String,
    resultTopic: String,
    scanTopic: String,
    payloadAuditTopic: String,
    dlqSuffix: String,
    scanConsumer: String,
    resultConsumer: String,
    payloadAuditConsumer: String,
    loadConsumer: String,
    maxPollRecords: Int,
    pollTimeoutMs: Long
) derives ConfigReader

final case class CronConfig(
    idleSleepMs: Int,
    idleSleepBackoffMs: Int,
    dispatchBatchSize: Int,
    ingestBatchSize: Int,
    scanMaxAttempts: Int,
    scanRetryBackoffSeconds: Int,
    batchDispatchLeaseSeconds: Int,
    batchMaxAttempts: Int,
    heartbeatLogIntervalMs: Int,
    schemaRefreshIntervalSeconds: Int,
    scanFetchCount: Int,
    resultFetchCount: Int
) derives ConfigReader

final case class IngestConfig(
    streamNames: List[String],
    loadStreamNames: List[String]
) derives ConfigReader

final case class BackpressureConfig(
    budgetMultiplier: Int,
    adaptivePullChangeThreshold: Int,
    adaptivePullMinRestartIntervalMs: Int
) derives ConfigReader

final case class SystemRegistryConfig(
    knownOrigins: List[String]
) derives ConfigReader

final case class HttpConfig(
    port: Int
)

object HttpConfig:
  given ConfigReader[HttpConfig] =
    ConfigReader[Int].emap { port =>
      if port < 0 || port > 65535 then
        Left(pureconfig.error.CannotConvert(port.toString, "Int",
          s"Port must be between 0 and 65535, got $port"))
      else
        Right(HttpConfig(port))
    }

final case class SyncConfig(
    outboxDir: String
) derives ConfigReader

object AppConfig:
  def load: AppConfig =
    pureconfig.ConfigSource.default.loadOrThrow[AppConfig]
