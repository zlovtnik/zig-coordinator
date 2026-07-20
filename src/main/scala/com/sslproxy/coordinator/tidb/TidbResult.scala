package com.sslproxy.coordinator.tidb

import io.circe.Encoder
import io.circe.generic.semiauto.*

/** Result published to `sync.oracle.result`. Ported from OracleResult. */
final case class TidbResult(
    jobId: String,
    batchId: String,
    status: String,
    rowCount: Int,
    checksum: String,
    retryable: Boolean,
    errorClass: String,
    errorText: String,
    finishedAt: String
)

object TidbResult:
  given Encoder[TidbResult] = deriveEncoder[TidbResult]

  def success(jobId: String, batchId: String, rowCount: Int, checksum: String, finishedAt: String): TidbResult =
    TidbResult(
      jobId = jobId,
      batchId = batchId,
      status = "success",
      rowCount = rowCount,
      checksum = checksum,
      retryable = false,
      errorClass = "",
      errorText = "",
      finishedAt = finishedAt
    )

  def failure(
      jobId: String,
      batchId: String,
      errorClass: TidbErrorClass,
      errorText: String,
      finishedAt: String
  ): TidbResult =
    TidbResult(
      jobId = if jobId == null then "" else jobId,
      batchId = if batchId == null then "" else batchId,
      status = "failed",
      rowCount = 0,
      checksum = "",
      retryable = errorClass == TidbErrorClass.Retryable,
      errorClass = errorClass.wireValue,
      errorText = if errorText == null then "" else errorText,
      finishedAt = finishedAt
    )