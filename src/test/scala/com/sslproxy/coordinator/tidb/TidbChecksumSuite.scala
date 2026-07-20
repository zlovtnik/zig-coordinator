package com.sslproxy.coordinator.tidb

import munit.*

class TidbChecksumSuite extends FunSuite:

  test("checksum is stable for same input"):
    val payload = """{"test": "data"}"""
    val a = TidbChecksum.checksum(TidbSinkTarget.ProxyEvents, payload)
    val b = TidbChecksum.checksum(TidbSinkTarget.ProxyEvents, payload)
    assertEquals(a, b)

  test("checksum differs for different targets"):
    val payload = """{"test": "data"}"""
    val a = TidbChecksum.checksum(TidbSinkTarget.ProxyEvents, payload)
    val b = TidbChecksum.checksum(TidbSinkTarget.WirelessAuditFrames, payload)
    assertNotEquals(a, b)

  test("checksum differs for different payloads"):
    val a = TidbChecksum.checksum(TidbSinkTarget.ProxyEvents, """{"a": 1}""")
    val b = TidbChecksum.checksum(TidbSinkTarget.ProxyEvents, """{"a": 2}""")
    assertNotEquals(a, b)

  test("checksum is non-empty"):
    val result = TidbChecksum.checksum(TidbSinkTarget.ProxyEvents, """{"test": "data"}""")
    assert(result.nonEmpty)
