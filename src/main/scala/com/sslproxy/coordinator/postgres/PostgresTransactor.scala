package com.sslproxy.coordinator.postgres

import cats.effect.{IO, Resource}
import doobie.hikari.HikariTransactor
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

final case class PostgresConfig(
    jdbcUrl: String,
    user: String,
    password: String,
    poolSize: Int,
    connectionTimeoutMs: Long,
    poolName: String
)

object PostgresTransactor:
  private val log = LoggerFactory.getLogger(getClass)

  private val blockingEc: ExecutionContext =
    ExecutionContext.fromExecutor(
      java.util.concurrent.Executors.newCachedThreadPool { r =>
        val t = new Thread(r, "doobie-postgres-pool")
        t.setDaemon(true)
        t
      }
    )

  def resource(cfg: PostgresConfig): Resource[IO, HikariTransactor[IO]] =
    HikariTransactor
      .newHikariTransactor[IO](
        driverClassName = "org.postgresql.Driver",
        url = cfg.jdbcUrl,
        user = cfg.user,
        pass = cfg.password,
        connectEC = blockingEc
      )
      .flatMap { xa =>
        Resource.eval(
          IO {
            xa.kernel.setMaximumPoolSize(cfg.poolSize)
            xa.kernel.setConnectionTimeout(cfg.connectionTimeoutMs)
            xa.kernel.setPoolName(cfg.poolName)
            xa.kernel.setConnectionTestQuery("SELECT 1")
            log.info("PostgresTransactor: pool allocated url={} pool={}",
              cfg.jdbcUrl.replaceAll("password=[^&]+", "password=***"), cfg.poolName)
            xa
          }
        )
      }
