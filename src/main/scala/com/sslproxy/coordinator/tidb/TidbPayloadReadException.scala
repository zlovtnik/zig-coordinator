package com.sslproxy.coordinator.tidb

class TidbPayloadReadException(message: String, cause: Throwable) extends Exception(message, cause):
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(cause.getMessage, cause)
