package com.sslproxy.coordinator.tidb

final case class TidbConfig(
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String,
    poolSize: Int,
    connectionTimeoutMs: Long,
    statementTimeoutSecs: Int,
    enabled: Boolean,
    warnOnly: Boolean
)

object TidbConfig:
  val DefaultHost: String = "127.0.0.1"
  val DefaultPort: Int = 4000
  val DefaultDatabase: String = "coordinator"
  val DefaultUser: String = "root"
  val DefaultPassword: String = ""
  val DefaultPoolSize: Int = 10
  val DefaultConnectionTimeoutMs: Long = 5000L
  val DefaultStatementTimeoutSecs: Int = 30

  def fromEnv: TidbConfig =
    TidbConfig(
      host = sys.env.getOrElse("TIDB_HOST", DefaultHost),
      port = sys.env.get("TIDB_PORT").flatMap(v => scala.util.Try(v.toInt).toOption).getOrElse(DefaultPort),
      database = sys.env.getOrElse("TIDB_DATABASE", DefaultDatabase),
      user = sys.env.getOrElse("TIDB_USER", DefaultUser),
      password = sys.env.getOrElse("TIDB_PASSWORD", DefaultPassword),
      poolSize = sys.env.get("TIDB_POOL_SIZE").flatMap(v => scala.util.Try(v.toInt).toOption).getOrElse(DefaultPoolSize),
      connectionTimeoutMs = sys.env.get("TIDB_CONNECTION_TIMEOUT_MS")
        .flatMap(v => scala.util.Try(v.toLong).toOption).getOrElse(DefaultConnectionTimeoutMs),
      statementTimeoutSecs = sys.env.get("TIDB_STATEMENT_TIMEOUT_SECS")
        .flatMap(v => scala.util.Try(v.toInt).toOption).getOrElse(DefaultStatementTimeoutSecs),
      enabled = sys.env.get("TIDB_ENABLED").forall(v => v.equalsIgnoreCase("true") || v == "1"),
      warnOnly = sys.env.get("TIDB_WARN_ONLY").exists(v => v.equalsIgnoreCase("true") || v == "1")
    )

  def jdbcUrl(config: TidbConfig): String =
    s"jdbc:mysql://${config.host}:${config.port}/${config.database}?rewriteBatchedStatements=true&useSSL=false&allowPublicKeyRetrieval=true"
