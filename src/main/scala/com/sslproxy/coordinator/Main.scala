package com.sslproxy.coordinator

import cats.effect.{IO, IOApp, Resource}
import cats.syntax.all.*
import com.comcast.ip4s.*
import com.sslproxy.coordinator.config.AppConfig
import com.sslproxy.coordinator.cron.CronScheduler
import com.sslproxy.coordinator.db.DoobieTransactor
import com.sslproxy.coordinator.http.HealthRoutes
import com.sslproxy.coordinator.kafka.{ConsumerStream, KafkaComponents, TidbLoadStream}
import com.sslproxy.coordinator.sink.*
import com.sslproxy.coordinator.tidb.*
import fs2.kafka.*
import org.http4s.ember.server.EmberServerBuilder
import org.slf4j.LoggerFactory

import scala.concurrent.duration.*

object Main extends IOApp.Simple:
  private val log = LoggerFactory.getLogger(getClass)

  private def toOldTidbConfig(c: com.sslproxy.coordinator.config.TiDbConfig): TidbConfig =
    TidbConfig(
      host = c.host, port = c.port, database = c.database, user = c.user,
      password = c.password, poolSize = c.poolSize,
      connectionTimeoutMs = c.connectionTimeoutMs,
      statementTimeoutSecs = c.statementTimeoutSecs,
      enabled = c.enabled, warnOnly = c.warnOnly, sslMode = c.sslMode
    )

  private def toOldKafkaConfig(c: com.sslproxy.coordinator.config.KafkaCfg): com.sslproxy.coordinator.config.KafkaConfig =
    com.sslproxy.coordinator.config.KafkaConfig(
      bootstrapServers = c.bootstrapServers, loadTopic = c.loadTopic,
      resultTopic = c.resultTopic, dlqSuffix = c.dlqSuffix,
      consumerGroup = c.consumerGroup, maxPollRecords = c.maxPollRecords,
      pollTimeoutMs = c.pollTimeoutMs
    )

  override def run: IO[Unit] =
    val cfg = AppConfig.load

    if !cfg.tidb.enabled then
      log.warn("event=startup status=disabled tidb_sink=disabled")
      IO.println("TiDB sink disabled (set TIDB_ENABLED=true to enable)").void
    else
      val oldTiDbCfg = toOldTidbConfig(cfg.tidb)
      val oldKafkaCfg = toOldKafkaConfig(cfg.kafka)

      val transactorResource: Resource[IO, (TidbTransactor, doobie.hikari.HikariTransactor[IO])] =
        for
          oldTx <- TidbTransactor.resource(oldTiDbCfg)
          newTx <- DoobieTransactor.resource(cfg.tidb)
        yield (oldTx, newTx)

      val appResource: Resource[IO, Unit] =
        transactorResource.flatMap { case (oldTx, newTx) =>
          val preflight = new TidbSchemaPreflight(oldTx, oldTiDbCfg)
          val startChecks = Resource.eval(preflight.validate())

          startChecks.flatMap { _ =>
            KafkaComponents.resource(oldKafkaCfg).flatMap { kafka =>
              val payloadResolver = new TidbPayloadResolver(cfg.sync.outboxDir)
              val handler = new TidbLoadHandler(payloadResolver, TidbTransformService, oldTx, TidbClock)

              val genericSink = new GenericTiDbSink(newTx)
              val schemaIntrospector = new SchemaIntrospector(newTx, cfg.tidb.database, cfg.cron.schemaRefreshIntervalSeconds.seconds)
              val registry = new SystemRegistry(cfg.systemRegistry)

              val sinkPipe = new SinkPipe(genericSink, schemaIntrospector, registry, dl =>
                val dlqTopic = cfg.kafka.loadTopic + cfg.kafka.dlqSuffix
                val record = ProducerRecord(dlqTopic, dl.table, dl.toDlqJson)
                kafka.producer.produce(ProducerRecords.one(record)).flatten.void
              )

              val cronScheduler = new CronScheduler(cfg.cron, newTx, schemaIntrospector)

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
                val streams =
                  TidbLoadStream.run(kafka, handler)
                    .merge(ConsumerStream.run(cfg.kafka, sinkPipe, kafka.producer))
                    .merge(cronScheduler.mainLoop)
                    .merge(cronScheduler.schemaRefresher)

                Resource.make(streams.compile.drain.start)(_.cancel).void
              }
            }
          }
        }

      log.info("event=startup status=starting tidb_host={} tidb_port={} tidb_database={}",
        cfg.tidb.host, cfg.tidb.port, cfg.tidb.database)

      appResource.useForever
