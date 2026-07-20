package com.sslproxy.coordinator.model

import com.sslproxy.coordinator.sink.ColValue

final case class SystemContext(
    origin: String,
    destination: String
):
  def asColumns: Map[String, ColValue] = Map(
    "origin_system" -> ColValue.VStr(origin),
    "destination_system" -> ColValue.VStr(destination)
  )
