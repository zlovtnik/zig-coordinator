package com.sslproxy.coordinator.tidb

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** SHA-256 checksum construction. Ported from OracleChecksum. */
object TidbChecksum:

  def checksum(target: TidbSinkTarget, payload: String): String =
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(target.checksumTag.getBytes(StandardCharsets.UTF_8))
    digest.update(0.toByte)
    digest.update(payload.getBytes(StandardCharsets.UTF_8))
    digest.digest().map("%02x".format(_)).mkString