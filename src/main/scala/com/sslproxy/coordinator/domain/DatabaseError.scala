package com.sslproxy.coordinator.domain

sealed trait DatabaseError:
  def operation: String
  def cause: Throwable
  def message: String

object DatabaseError:
  final case class Permanent(operation: String, cause: Throwable, message: String) extends DatabaseError
  final case class Retryable(operation: String, cause: Throwable, message: String) extends DatabaseError
