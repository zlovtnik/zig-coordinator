package com.sslproxy.coordinator.sink

import io.circe.Json

enum ColValue:
  case VStr(value: String)
  case VLong(value: Long)
  case VDecimal(value: BigDecimal)
  case VDouble(value: Double)
  case VBool(value: Boolean)
  case VJson(value: String)
  case VNull

object ColValue:
  def fromJsonField(json: Json): ColValue =
    if json.isNull then VNull
    else if json.isString then VStr(json.asString.get)
    else
      json.asNumber match
        case Some(n) =>
          n.toLong match
            case Some(l) => VLong(l)
            case None    => n.toBigDecimal match
              case Some(bd) => VDecimal(bd)
              case None     => VDouble(n.toDouble)
        case None =>
          if json.isBoolean then VBool(json.asBoolean.get)
          else VJson(json.noSpaces)
