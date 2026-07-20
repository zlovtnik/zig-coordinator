package com.sslproxy.coordinator.db

import cats.effect.{IO, Resource}
import com.sslproxy.coordinator.config.TiDbConfig
import doobie.hikari.HikariTransactor
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

object DoobieTransactor:
  private val log = LoggerFactory.getLogger(getClass)

  private val blockingEc: ExecutionContext =
    ExecutionContext.fromExecutor(
      java.util.concurrent.Executors.newCachedThreadPool { r =>
        val t = new Thread(r, "doobie-tidb-pool")
        t.setDaemon(true)
        t
      }
    )

  def resource(cfg: TiDbConfig): Resource[IO, HikariTransactor[IO]] =
    val jdbcUrl = s"jdbc:mysql://${cfg.host}:${cfg.port}/${cfg.database}?rewriteBatchedStatements=true" +
      (cfg.sslMode match
        case "DISABLED" => "&useSSL=false&allowPublicKeyRetrieval=true"
        case mode       => s"&sslMode=$mode")

    HikariTransactor
      .newHikariTransactor[IO](
        driverClassName = "com.mysql.cj.jdbc.Driver",
        url = jdbcUrl,
        user = cfg.user,
        pass = cfg.password,
        connectEC = blockingEc
      )
      .flatMap { xa =>
        Resource.eval(
          IO {
            xa.kernel.setMaximumPoolSize(cfg.poolSize)
            xa.kernel.setConnectionTimeout(cfg.connectionTimeoutMs)
            xa.kernel.setPoolName("doobie-tidb-pool")
            xa.kernel.setConnectionTestQuery("SELECT 1")
            xa.kernel.addDataSourceProperty("cachePrepStmts", "true")
            xa.kernel.addDataSourceProperty("prepStmtCacheSize", "250")
            xa.kernel.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
            log.info("DoobieTransactor: HikariCP pool allocated to {}:{}/{}",
              cfg.host, cfg.port, cfg.database)
            xa
          }
        )
      }
