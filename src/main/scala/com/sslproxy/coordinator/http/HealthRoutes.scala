package com.sslproxy.coordinator.http

import cats.effect.IO
import com.sslproxy.coordinator.tidb.TidbTransactor
import io.circe.Json
import org.http4s.HttpRoutes
import org.http4s.dsl.io.*
import org.http4s.circe.*

class HealthRoutes(transactor: TidbTransactor):

  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "actuator" / "health" =>
      transactor.healthCheck.flatMap { healthy =>
        val status = if healthy then "UP" else "DOWN"
        val json = Json.obj(
          "status" -> Json.fromString(status),
          "components" -> Json.obj(
            "tidb" -> Json.obj("status" -> Json.fromString(status))
          )
        )
        if healthy then Ok(json)
        else ServiceUnavailable(json)
      }

    case GET -> Root / "health" =>
      transactor.healthCheck.flatMap { healthy =>
        val status = if healthy then "UP" else "DOWN"
        val json = Json.obj("status" -> Json.fromString(status))
        if healthy then Ok(json)
        else ServiceUnavailable(json)
      }
  }
