package com.sslproxy.coordinator.tidb

import cats.effect.IO
import io.circe.Json
import org.slf4j.LoggerFactory

class TidbLoadHandler(
    payloadResolver: TidbPayloadResolver,
    transformService: TidbTransformService.type,
    sink: TidbSink,
    clock: TidbClock.type
):
  import TidbLoadHandler.log

  def handle(load: TidbLoad): IO[TidbResult] =
    val finishedAt = clock.nowRfc3339
    (for
      resolved <- repairPayloadRefIfNeeded(load)
      _        <- validateLoad(resolved)
      target   <- resolveTarget(resolved)
      payload  <- resolvePayload(resolved)
      rows     <- parseRows(target, payload)
      result   <- transformAndInsert(resolved, target, rows)
      checksum  = TidbChecksum.checksum(target, payload)
      finalResult = result match
        case Left(err) => err
        case Right(rowCount) =>
          if rowCount > Int.MaxValue then
            TidbResult.failure(resolved.jobId, resolved.batchId, TidbErrorClass.Permanent,
              "inserted row count exceeds i32 limit", finishedAt)
          else
            TidbResult.success(resolved.jobId, resolved.batchId, rowCount.toInt, checksum, finishedAt)
    yield finalResult).handleError { err =>
      val errorClass = err match
        case _: TidbPayloadReadException => TidbErrorClass.Retryable
        case _: IllegalArgumentException => TidbErrorClass.Permanent
        case _: io.circe.ParsingFailure  => TidbErrorClass.Permanent
        case _                           => TidbErrorClass.classify(err)
      log.error("event=tidb_load status=failed batch_id={} stream_name={} error_class={} error=\"{}\"",
        load.batchId, load.streamName, errorClass.wireValue, sanitize(err.getMessage))
      TidbResult.failure(load.jobId, load.batchId, errorClass, err.getMessage, finishedAt)
    }

  private def resolveTarget(load: TidbLoad): IO[TidbSinkTarget] =
    TidbSinkTarget.fromStreamName(load.streamName) match
      case Some(target) => IO.pure(target)
      case None         => IO.raiseError(IllegalArgumentException(s"unsupported stream_name ${load.streamName}"))

  private def resolvePayload(load: TidbLoad): IO[String] =
    IO.blocking(payloadResolver.resolvePayload(load.payloadRef))

  private def parseRows(target: TidbSinkTarget, payload: String): IO[List[Json]] =
    IO.blocking(payloadResolver.payloadRows(target, payload))

  private def transformAndInsert(load: TidbLoad, target: TidbSinkTarget, rows: List[Json]): IO[Either[TidbResult, Long]] =
    val transformed = transformService.transform(target, rows)
    val rowCount = transformed.inputRowCount(target)
    if rowCount == 0 then IO.pure(Right(0L))
    else
      val insertIO: IO[Long] = target match
        case TidbSinkTarget.ProxyEvents =>
          sink.insertProxyEvents(load.batchId, transformed.proxyEvents, transformed.blockedEvents)
        case TidbSinkTarget.ProxyPayloadAudit =>
          sink.insertProxyPayloadAudit(load.batchId, transformed.proxyPayloadAudit)
        case TidbSinkTarget.WirelessAuditFrames =>
          sink.insertWirelessAuditFrames(load.batchId, transformed.wirelessAuditFrames)
        case TidbSinkTarget.WirelessBandwidth =>
          sink.insertWirelessBandwidth(load.batchId, transformed.wirelessBandwidth)
        case TidbSinkTarget.WirelessRogueAp =>
          sink.insertWirelessRogueAp(load.batchId, transformed.wirelessRogueAp)
        case TidbSinkTarget.WirelessDeauthFlood =>
          sink.insertWirelessDeauthFlood(load.batchId, transformed.wirelessDeauthFlood)
        case TidbSinkTarget.WirelessSignalAnomaly =>
          sink.insertWirelessSignalAnomaly(load.batchId, transformed.wirelessSignalAnomaly)
        case TidbSinkTarget.WirelessPmfAttack =>
          sink.insertWirelessPmfAttack(load.batchId, transformed.wirelessPmfAttack)
        case TidbSinkTarget.WirelessClientInventory =>
          sink.insertWirelessClientInventory(load.batchId, transformed.wirelessClientInventory)
        case TidbSinkTarget.WirelessProbeRequests =>
          sink.insertWirelessProbeRequests(load.batchId, transformed.wirelessProbeRequests)

      insertIO.attempt.map {
        case Right(count) => Right(count)
        case Left(err)    => Left(buildFailureResult(load, err))
      }

  private def buildFailureResult(load: TidbLoad, err: Throwable): TidbResult =
    val errorClass = err match
      case _: TidbPayloadReadException => TidbErrorClass.Retryable
      case _: IllegalArgumentException => TidbErrorClass.Permanent
      case _                           => TidbErrorClass.classify(err)
    TidbResult.failure(load.jobId, load.batchId, errorClass, err.getMessage, clock.nowRfc3339)

  private def repairPayloadRefIfNeeded(load: TidbLoad): IO[TidbLoad] =
    validateLoadMetadata(load)
    if load.payloadRef.nonEmpty then IO.pure(load)
    else IO.raiseError(IllegalArgumentException("payload_ref must not be empty (repair from database not available in standalone TiDB sink)"))

  private def validateLoad(load: TidbLoad): IO[Unit] =
    validateLoadMetadata(load)
    if load.payloadRef.isBlank then
      IO.raiseError(IllegalArgumentException("payload_ref must not be empty"))
    else IO.unit

  private def validateLoadMetadata(load: TidbLoad): Unit =
    if load.jobId.isBlank then
      throw IllegalArgumentException("job_id must not be empty")
    if load.batchId.isBlank then
      throw IllegalArgumentException("batch_id must not be empty")
    if load.streamName.isBlank then
      throw IllegalArgumentException("stream_name must not be empty")

  private def sanitize(msg: String): String =
    if msg == null then "" else msg.replace('\n', ' ').replace('\r', ' ')

object TidbLoadHandler:
  private val log = LoggerFactory.getLogger(getClass)
