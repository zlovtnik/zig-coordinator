package com.sslproxy.coordinator.kafka

import com.sslproxy.coordinator.model.SystemContext
import com.sslproxy.coordinator.sink.{ColValue, Row}
import io.circe.Json
import io.circe.parser.parse as parseJson

final case class KafkaEnvelope(
    table: String,
    row: Row,
    ctx: SystemContext
)

object KafkaEnvelope:
  def decode(json: String): Either[Throwable, KafkaEnvelope] =
    for
      parsed  <- parseJson(json)
      obj     <- parsed.asObject.toRight(IllegalArgumentException("envelope must be a JSON object"))
      table   <- obj("table").flatMap(_.asString).toRight(IllegalArgumentException("envelope missing 'table' field"))
      data    <- obj("data").toRight(IllegalArgumentException("envelope missing 'data' field"))
      origin  <- obj("origin").flatMap(_.asString).toRight(IllegalArgumentException("envelope missing 'origin' field"))
      dest    <- obj("destination").flatMap(_.asString).toRight(IllegalArgumentException("envelope missing 'destination' field"))
      dataObj <- data.asObject.toRight(IllegalArgumentException("envelope field 'data' must be a JSON object"))
      fields   = dataObj.toMap
      row      = fields.view.mapValues(ColValue.fromJsonField).toMap
    yield KafkaEnvelope(table = table, row = row, ctx = SystemContext(origin = origin, destination = dest))
