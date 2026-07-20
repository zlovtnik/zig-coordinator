package com.sslproxy.coordinator.errors

import com.sslproxy.coordinator.model.SystemContext
import com.sslproxy.coordinator.sink.{ColValue, Row}
import io.circe.Json
import io.circe.parser.parse

import java.time.{OffsetDateTime, ZoneOffset}

final case class DeadLetter(
    table: String,
    ctx: SystemContext,
    row: Row,
    error: String,
    timestamp: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
):
  def toDlqJson: String =
    val rowFields = Json.obj(row.map { case (k, v) =>
      k -> colValueToJson(v)
    }.toSeq*)
    Json.obj(
      "table" -> Json.fromString(table),
      "origin" -> Json.fromString(ctx.origin),
      "destination" -> Json.fromString(ctx.destination),
      "error" -> Json.fromString(error),
      "timestamp" -> Json.fromString(timestamp.toString),
      "row" -> rowFields
    ).noSpaces

  private def colValueToJson(v: ColValue): Json =
    v match
      case ColValue.VNull      => Json.Null
      case ColValue.VStr(s)    => Json.fromString(s)
      case ColValue.VLong(n)   => Json.fromLong(n)
      case ColValue.VDecimal(bd) => Json.fromBigDecimal(bd)
      case ColValue.VDouble(d) => Json.fromDoubleOrNull(d)
      case ColValue.VBool(b)   => Json.fromBoolean(b)
      case ColValue.VJson(j)   => parse(j).getOrElse(Json.fromString(j))
