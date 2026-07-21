package com.sslproxy.coordinator.util

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

object Sha256Utils:
  def sha256Hex(input: String): String =
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(input.getBytes(StandardCharsets.UTF_8))
    digest.digest().map("%02x".format(_)).mkString

  def sha256Hex(bytes: Array[Byte]): String =
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(bytes)
    digest.digest().map("%02x".format(_)).mkString
