package com.sslproxy.coordinator.sink

import io.circe.Json

enum ColValue:
  case VStr(value: String)
  case VLong(value: Long)
  case VDouble(value: Double)
  case VBool(value: Boolean)
  case VJson(value: String)
  case VNull

object ColValue:
  def fromJsonField(json: Json): ColValue =
    if json.isNull then VNull
    else if json.isString then VStr(json.asString.get)
    else if json.isNumber then
      json.asNumber.flatMap(_.toLong) match
        case Some(l) => VLong(l)
        case None    => VDouble(json.asNumber.get.toDouble)
    else if json.isBoolean then VBool(json.asBoolean.get)
    else VJson(json.noSpaces)
