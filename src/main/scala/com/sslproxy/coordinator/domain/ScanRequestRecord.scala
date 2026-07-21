package com.sslproxy.coordinator.domain

import com.sslproxy.coordinator.util.Sha256Utils
import io.circe.{Decoder, HCursor}
import io.circe.parser.decode

import java.nio.charset.StandardCharsets

/** Locked `sync.scan.request` wire contract plus the resolved payload when it is
  * already available locally. `payloadJson` may be empty for an outbox-backed
  * request; `payloadRef` is always the source of truth for dispatch.
  */
final case class ScanRequestRecord(
    requestJson: String,
    payloadJson: String,
    payloadSha256: String,
    streamName: String,
    dedupeKey: String,
    observedAt: String,
    payloadRef: String = ""
)

object ScanRequestRecord:
  private final case class Wire(
      streamName: String,
      dedupeKey: String,
      payloadRef: String,
      observedAt: String
  )

  private given Decoder[Wire] = (cursor: HCursor) =>
    for
      streamName <- cursor.downField("stream_name").as[String]
      dedupeKey <- cursor.downField("dedupe_key").as[String]
      payloadRef <- cursor.downField("payload_ref").as[String]
      observedAt <- cursor.downField("observed_at").as[String]
    yield Wire(streamName, dedupeKey, payloadRef, observedAt)

  def decodeWire(rawJson: String): Either[Throwable, ScanRequestRecord] =
    decode[Wire](rawJson).flatMap { wire =>
      if wire.streamName.isBlank then Left(IllegalArgumentException("scan request stream_name must not be empty"))
      else if wire.dedupeKey.isBlank then Left(IllegalArgumentException("scan request dedupe_key must not be empty"))
      else if wire.payloadRef.isBlank then Left(IllegalArgumentException("scan request payload_ref must not be empty"))
      else if wire.observedAt.isBlank then Left(IllegalArgumentException("scan request observed_at must not be empty"))
      else
        val payloadSha256 = Sha256Utils.sha256Hex(rawJson.getBytes(StandardCharsets.UTF_8))
        Right(
          ScanRequestRecord(
            requestJson = rawJson,
            payloadJson = "",
            payloadSha256 = payloadSha256,
            streamName = wire.streamName,
            dedupeKey = wire.dedupeKey,
            observedAt = wire.observedAt,
            payloadRef = wire.payloadRef
          )
        )
    }
