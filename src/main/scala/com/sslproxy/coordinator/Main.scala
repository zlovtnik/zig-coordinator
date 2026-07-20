package com.sslproxy.coordinator

import cats.effect.{IO, IOApp, Resource}
import cats.syntax.all.*
import com.comcast.ip4s.*
import com.sslproxy.coordinator.config.AppConfig
import com.sslproxy.coordinator.cron.CronScheduler
import com.sslproxy.coordinator.dispatch.{BackpressureService, BatchDispatchService}
import com.sslproxy.coordinator.http.HealthRoutes
import com.sslproxy.coordinator.ingest.PayloadAuditConsumer
import com.sslproxy.coordinator.kafka.{ConsumerStream, KafkaComponents, TidbLoadStream, WirelessConsumerService}
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.sink.*
import com.sslproxy.coordinator.tidb.*
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.Transactor
import fs2.kafka.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.http4s.ember.server.EmberServerBuilder
import org.slf4j.LoggerFactory

import scala.concurrent.duration.*

object Main extends IOApp.Simple:
  private val log = LoggerFactory.getLogger(getClass)

  private val blockingEc: scala.concurrent.ExecutionContext =
    scala.concurrent.ExecutionContext.fromExecutor(
      java.util.concurrent.Executors.newCachedThreadPool { r =>
        val t = new Thread(r, "doobie-tidb-pool")
        t.setDaemon(true)
        t
      }
    )

  override def run: IO[Unit] =
    val cfg = AppConfig.load
    val meterRegistry = new SimpleMeterRegistry()
    val metrics = new CoordinatorMetrics(meterRegistry)

    if !cfg.tidb.enabled then
      log.warn("event=startup status=disabled tidb_sink=disabled")
      IO.println("TiDB sink disabled (set TIDB_ENABLED=true to enable)").void
    else
      val tiDbPoolResource: Resource[IO, HikariDataSource] =
        Resource.make(
          IO.blocking {
            val hc = new HikariConfig()
            hc.setJdbcUrl(TidbTransactor.jdbcUrl(cfg.tidb))
            hc.setUsername(cfg.tidb.user)
            hc.setPassword(cfg.tidb.password)
            hc.setMaximumPoolSize(cfg.tidb.poolSize)
            hc.setConnectionTimeout(cfg.tidb.connectionTimeoutMs)
            hc.setDriverClassName("com.mysql.cj.jdbc.Driver")
            hc.setPoolName("tidb-pool")
            hc.setAutoCommit(true)
            hc.setConnectionTestQuery("SELECT 1")
            hc.addDataSourceProperty("cachePrepStmts", "true")
            hc.addDataSourceProperty("prepStmtCacheSize", "250")
            hc.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
            val ds = new HikariDataSource(hc)
            log.info("TiDB pool allocated to {}:{}/{}",
              cfg.tidb.host, cfg.tidb.port, cfg.tidb.database)
            ds
          }
        ) { ds => IO.blocking(ds.close()) }

      val appResource: Resource[IO, Unit] =
        tiDbPoolResource.flatMap { tiDbDs =>
          val oldTx = TidbTransactor.fromDataSource(tiDbDs, cfg.tidb)
          val tiDbDoobieTx = Transactor.fromDataSource[IO](tiDbDs, blockingEc)
          val preflight = new TidbSchemaPreflight(oldTx, cfg.tidb)
          val tiDbRepo = new TidbRepository(tiDbDoobieTx)

          Resource.eval(preflight.validate()).flatMap { _ =>
            KafkaComponents.resource(cfg.kafka).flatMap { kafka =>
              Resource.eval(SchemaIntrospector(tiDbDoobieTx, cfg.tidb.database, cfg.cron.schemaRefreshIntervalSeconds.seconds)).flatMap { schemaIntrospector =>
                val payloadResolver = new TidbPayloadResolver(cfg.sync.outboxDir)
                val handler = new TidbLoadHandler(payloadResolver, TidbTransformService, oldTx, TidbClock)

                val genericSink = new GenericTiDbSink(tiDbDoobieTx)
                val registry = new SystemRegistry(cfg.systemRegistry)

                val sinkPipe = new SinkPipe(genericSink, schemaIntrospector, registry, dl =>
                  val dlqTopic = cfg.kafka.loadTopic + cfg.kafka.dlqSuffix
                  val record = ProducerRecord(dlqTopic, dl.table, dl.toDlqJson)
                  kafka.producer.produce(ProducerRecords.one(record)).flatten.void
                )

                val backpressureService = new BackpressureService(
                  cfg.backpressure, cfg.cron.ingestBatchSize,
                  tiDbRepo.pendingLedgerCount(), metrics
                )

                val batchDispatchService = new BatchDispatchService(
                  tiDbRepo, kafka.producer, cfg.kafka, metrics,
                  cfg.ingest.loadStreamNames, cfg.cron.batchMaxAttempts
                )

                val cronScheduler = new CronScheduler(
                  cfg.cron, cfg.ingest, tiDbRepo,
                  backpressureService, batchDispatchService, metrics,
                  schemaIntrospector
                )

                val healthRoutes = new HealthRoutes(oldTx)

                val httpPort = Port.fromInt(cfg.http.port).getOrElse(
                  sys.error(s"Port ${cfg.http.port} validated by config but IP4s rejected it")
                )
                val serverResource = EmberServerBuilder.default[IO]
                  .withPort(httpPort)
                  .withHost(host"0.0.0.0")
                  .withHttpApp(healthRoutes.routes.orNotFound)
                  .build

                serverResource.flatMap { _ =>
                  val payloadAuditStream = PayloadAuditConsumer.stream(
                    cfg.kafka, tiDbRepo, metrics, kafka.producer
                  )

                  val wirelessStreams = WirelessConsumerService.allStreams(
                    cfg.wireless, cfg.kafka.bootstrapServers, tiDbRepo, kafka.producer
                  )

                  val streams =
                    TidbLoadStream.run(kafka, handler)
                      .merge(ConsumerStream.run(cfg.kafka, sinkPipe, kafka.producer))
                      .merge(cronScheduler.mainLoop)
                      .merge(cronScheduler.schemaRefresher)
                      .merge(payloadAuditStream)
                      .merge(wirelessStreams)

                  Resource.make(
                    tiDbRepo.ensureAllCursors(cfg.ingest.streamNames) *>
                      streams.compile.drain.start.flatMap { fiber =>
                        fiber.join.flatMap {
                          case outcome if !outcome.isSuccess =>
                            log.error("event=stream_fatal status=failed outcome={}", outcome)
                            IO.raiseError(RuntimeException("Background stream terminated unexpectedly"))
                          case _ => IO.unit
                        }.start.as(fiber)
                      }
                  )(_.cancel).void
                }
              }
            }
          }
        }

      log.info("event=startup status=starting tidb_host={} tidb_port={} tidb_database={}",
        cfg.tidb.host, cfg.tidb.port, cfg.tidb.database)

      appResource.useForever
