package com.sslproxy.coordinator.tidb

import io.circe.{Encoder, Json, JsonObject}

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
  given Encoder[TidbResult] = (r: TidbResult) =>
    Json.fromJsonObject(JsonObject(
      "job_id"      -> Json.fromString(r.jobId),
      "batch_id"    -> Json.fromString(r.batchId),
      "status"      -> Json.fromString(r.status),
      "row_count"   -> Json.fromInt(r.rowCount),
      "checksum"    -> Json.fromString(r.checksum),
      "retryable"   -> Json.fromBoolean(r.retryable),
      "error_class" -> Json.fromString(r.errorClass),
      "error_text"  -> Json.fromString(r.errorText),
      "finished_at" -> Json.fromString(r.finishedAt)
    ))

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
