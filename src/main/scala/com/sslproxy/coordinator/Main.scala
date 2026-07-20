package com.sslproxy.coordinator

import cats.effect.{IO, IOApp, Resource}
import com.comcast.ip4s.*
import com.sslproxy.coordinator.config.CoordinatorConfig
import com.sslproxy.coordinator.http.HealthRoutes
import com.sslproxy.coordinator.kafka.{KafkaComponents, TidbLoadStream}
import com.sslproxy.coordinator.tidb.*
import org.http4s.ember.server.EmberServerBuilder
import org.slf4j.LoggerFactory

object Main extends IOApp.Simple:
  private val log = LoggerFactory.getLogger(getClass)

  override def run: IO[Unit] =
    val config = CoordinatorConfig.fromEnv

    if !config.tidb.enabled then
      log.warn("event=startup status=disabled tidb_sink=disabled")
      IO.println("TiDB sink disabled (set TIDB_ENABLED=true to enable)").void
    else
      log.info("event=startup status=starting tidb_host={} tidb_port={} tidb_database={}",
        config.tidb.host, config.tidb.port, config.tidb.database)

      val program: Resource[IO, Unit] =
        for
          transactor <- TidbTransactor.resource(config.tidb)
          _          <- Resource.eval(runPreflight(transactor, config))
          kafka      <- KafkaComponents.resource(config.kafka)
          _          <- Resource.eval(logStartup(config))

          payloadResolver = new TidbPayloadResolver(config.syncOutboxDir)
          handler = new TidbLoadHandler(payloadResolver, TidbTransformService, transactor, TidbClock)

          healthRoutes = new HealthRoutes(transactor)

          _ <- EmberServerBuilder.default[IO]
            .withPort(Port.fromInt(config.httpPort).get)
            .withHost(host"0.0.0.0")
            .withHttpApp(healthRoutes.routes.orNotFound)
            .build

          _ <- TidbLoadStream.run(kafka, handler).compile.drain.background
        yield ()

      program.useForever

  private def runPreflight(transactor: TidbTransactor, config: CoordinatorConfig): IO[Unit] =
    val preflight = new TidbSchemaPreflight(transactor, config.tidb)
    preflight.validate()

  private def logStartup(config: CoordinatorConfig): IO[Unit] =
    IO {
      log.info("event=startup config tidb={}:{}/{} kafka={} load_topic={} result_topic={}",
        config.tidb.host, config.tidb.port, config.tidb.database,
        config.kafka.bootstrapServers, config.kafka.loadTopic, config.kafka.resultTopic)
    }
