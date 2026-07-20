package com.sslproxy.coordinator.tidb

import io.circe.Decoder
import io.circe.generic.semiauto.*

/** Deserialized `sync.oracle.load` record. Ported from OracleLoad. */
final case class TidbLoad(
    jobId: String,
    batchId: String,
    batchNo: Option[Int],
    streamName: String,
    payloadRef: String,
    cursorStart: String,
    cursorEnd: String,
    attempt: Int
)

object TidbLoad:
  given Decoder[TidbLoad] = deriveDecoder[TidbLoad]