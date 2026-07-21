package com.sslproxy.coordinator.domain

import io.circe.parser.decode as circeDecode
import io.circe.syntax.*
import munit.*

class SyncLoadSuite extends FunSuite:

  private val loadJson = """{
    "job_id": "job-123",
    "batch_id": "batch-456",
    "batch_no": 1,
    "stream_name": "proxy.events",
    "payload_ref": "inline://json/eyJrZXkiOiAidmFsdWUifQ==",
    "cursor_start": "2026-07-20T12:00:00Z",
    "cursor_end": "2026-07-20T12:05:00Z",
    "attempt": 2
  }"""

  test("decode SyncLoad from JSON"):
    val result = circeDecode[SyncLoad](loadJson)
    assert(result.isRight)
    val load = result.toOption.get
    assertEquals(load.jobId, "job-123")
    assertEquals(load.batchId, "batch-456")
    assertEquals(load.batchNo, Some(1))
    assertEquals(load.streamName, "proxy.events")
    assertEquals(load.payloadRef, "inline://json/eyJrZXkiOiAidmFsdWUifQ==")
    assertEquals(load.cursorStart, "2026-07-20T12:00:00Z")
    assertEquals(load.cursorEnd, "2026-07-20T12:05:00Z")
    assertEquals(load.attempt, 2)

  test("encode SyncLoad to JSON"):
    val load = SyncLoad(
      jobId = "job-123",
      batchId = "batch-456",
      batchNo = Some(1),
      streamName = "proxy.events",
      payloadRef = "inline://json/eyJrZXkiOiAidmFsdWUifQ==",
      cursorStart = "2026-07-20T12:00:00Z",
      cursorEnd = "2026-07-20T12:05:00Z",
      attempt = 2
    )
    val json = load.asJson.noSpaces
    assert(json.contains("\"job_id\":\"job-123\""))
    assert(json.contains("\"batch_id\":\"batch-456\""))
    assert(json.contains("\"stream_name\":\"proxy.events\""))
    assert(json.contains("\"attempt\":2"))

  test("decode SyncLoad with missing optional batch_no"):
    val json = """{
      "job_id": "job-123",
      "batch_id": "batch-456",
      "stream_name": "proxy.events",
      "payload_ref": "inline://json/dGVzdA==",
      "cursor_start": "",
      "cursor_end": "",
      "attempt": 1
    }"""
    val result = circeDecode[SyncLoad](json)
    assert(result.isRight)
    assertEquals(result.toOption.get.batchNo, None)

  test("decode SyncLoad with null batch_no"):
    val json = """{
      "job_id": "job-123",
      "batch_id": "batch-456",
      "batch_no": null,
      "stream_name": "proxy.events",
      "payload_ref": "inline://json/dGVzdA==",
      "cursor_start": "",
      "cursor_end": "",
      "attempt": 1
    }"""
    val result = circeDecode[SyncLoad](json)
    assert(result.isRight)
    assertEquals(result.toOption.get.batchNo, None)

  test("reject SyncLoad without required fields"):
    val json = """{"job_id": "job-123"}"""
    val result = circeDecode[SyncLoad](json)
    assert(result.isLeft)

  test("encode/decode roundtrip preserves values"):
    val original = SyncLoad(
      jobId = "job-roundtrip",
      batchId = "batch-roundtrip",
      batchNo = None,
      streamName = "proxy.payload_audit",
      payloadRef = "inline://json/dGVzdA==",
      cursorStart = "2026-07-20T12:00:00Z",
      cursorEnd = "2026-07-20T12:05:00Z",
      attempt = 0
    )
    val json = original.asJson.noSpaces
    val decoded = circeDecode[SyncLoad](json)
    assert(decoded.isRight)
    assertEquals(decoded.toOption.get, original)
