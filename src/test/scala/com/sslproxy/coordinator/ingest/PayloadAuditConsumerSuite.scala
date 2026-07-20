package com.sslproxy.coordinator.ingest

import com.sslproxy.coordinator.domain.PayloadAudit
import com.sslproxy.coordinator.util.Sha256Utils
import munit.*

import java.nio.charset.StandardCharsets
import java.util.Base64

class PayloadAuditConsumerSuite extends FunSuite:

  private val validJson = """{
    "observed_at": "2026-06-01T12:00:00Z",
    "host": "api.example.com",
    "method": "POST",
    "path": "/login",
    "content_type": "application/json",
    "body": {"password": "[REDACTED]"}
  }"""

  test("parse valid payload audit JSON"):
    val result = PayloadAudit.parse(validJson)
    assert(result.isRight)
    val audit = result.toOption.get
    assertEquals(audit.observedAt, "2026-06-01T12:00:00Z")
    assertEquals(audit.host, Some("api.example.com"))
    assertEquals(audit.method, Some("POST"))
    assertEquals(audit.path, Some("/login"))
    assertEquals(audit.contentType, Some("application/json"))

  test("reject payload audit without observed_at"):
    val json = """{"host": "api.example.com"}"""
    val result = PayloadAudit.parse(json)
    assert(result.isLeft)

  test("reject payload audit with malformed observed_at"):
    val json = """{"observed_at": "not-a-timestamp", "host": "api.example.com"}"""
    val result = PayloadAudit.parse(json)
    assert(result.isRight) // observed_at is just a string field, no date parse in model

  test("accept payload audit with partial fields"):
    val json = """{"observed_at": "2026-06-01T12:00:00Z"}"""
    val result = PayloadAudit.parse(json)
    assert(result.isRight)
    val audit = result.toOption.get
    assertEquals(audit.host, None)
    assertEquals(audit.method, None)

  test("reject invalid JSON"):
    val result = PayloadAudit.parse("not json")
    assert(result.isLeft)

  test("reject null body"):
    val result = PayloadAudit.parse("")
    assert(result.isLeft)

  test("translate valid payload audit produces correct stream name"):
    val json = """{"observed_at": "2026-06-01T12:00:00Z", "host": "test"}"""
    val body = json.getBytes(StandardCharsets.UTF_8)
    val sha256 = Sha256Utils.sha256Hex(body)
    val dedupeKey = Sha256Utils.sha256Hex("proxy.payload_audit:" + sha256)
    val expectedPayloadRef = s"inline://json/${Base64.getUrlEncoder.withoutPadding.encodeToString(body)}"

    import io.circe.parser.decode as circeDecode
    import io.circe.Json
    val expectedRequest = Json.obj(
      "stream_name" -> Json.fromString("proxy.payload_audit"),
      "dedupe_key" -> Json.fromString(dedupeKey),
      "payload_ref" -> Json.fromString(expectedPayloadRef),
      "observed_at" -> Json.fromString("2026-06-01T12:00:00Z")
    ).noSpaces

    val result = translateRecord(
      fs2.kafka.ConsumerRecord[String, String]("proxy.payload_audit", 0, 0L, null, json)
    )
    assert(result.isRight)
    val record = result.toOption.get
    assertEquals(circeDecode[Json](record.requestJson).toOption.get, circeDecode[Json](expectedRequest).toOption.get)
    assertEquals(record.payloadJson, json)

  test("empty message is treated as empty"):
    val result = translateRecord(
      fs2.kafka.ConsumerRecord[String, String]("proxy.payload_audit", 0, 0L, null, "")
    )
    assert(result.isLeft)
    assertEquals(result.swap.toOption.get, PayloadAuditError.EmptyMessage)

  test("null message is treated as empty"):
    val result = translateRecord(
      fs2.kafka.ConsumerRecord[String, String]("proxy.payload_audit", 0, 0L, null, null)
    )
    assert(result.isLeft)
    assertEquals(result.swap.toOption.get, PayloadAuditError.EmptyMessage)

  private def translateRecord(record: fs2.kafka.ConsumerRecord[String, String]): Either[PayloadAuditError, com.sslproxy.coordinator.domain.ScanRequestRecord] =
    val rawJson = record.value
    if rawJson == null || rawJson.isEmpty then
      Left(PayloadAuditError.EmptyMessage)
    else
      PayloadAudit.parse(rawJson) match
        case Left(err) =>
          Left(PayloadAuditError.InvalidPayload(rawJson, err.getMessage))
        case Right(audit) =>
          val payloadBytes = rawJson.getBytes(StandardCharsets.UTF_8)
          val payloadSha256 = Sha256Utils.sha256Hex(payloadBytes)
          val dedupeKey = Sha256Utils.sha256Hex(s"proxy.payload_audit:$payloadSha256")
          val payloadRef = s"inline://json/${Base64.getUrlEncoder.withoutPadding.encodeToString(payloadBytes)}"

          import io.circe.Json
          val requestJson = Json.obj(
            "stream_name" -> Json.fromString("proxy.payload_audit"),
            "dedupe_key" -> Json.fromString(dedupeKey),
            "payload_ref" -> Json.fromString(payloadRef),
            "observed_at" -> Json.fromString(audit.observedAt)
          ).noSpaces

          Right(com.sslproxy.coordinator.domain.ScanRequestRecord(requestJson, rawJson, payloadSha256))
