package com.sslproxy.coordinator.postgres

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.config.IngestConfig
import org.slf4j.LoggerFactory

class CursorService(
    repo: CoordinatorRepository,
    config: IngestConfig
):
  import CursorService.log

  def ensureCursors(): IO[Unit] =
    config.streamNames
      .traverse_ { streamName =>
        repo.ensureCursor(streamName).flatMap {
          case Left(err) =>
            IO.raiseError(RuntimeException(
              s"Failed to ensure cursor for stream '$streamName': ${err.message}"))
          case Right(cursor) =>
            IO(log.info("event=cursor_init status=ok stream={} cursor={}", streamName, cursor))
        }
      }

object CursorService:
  private val log = LoggerFactory.getLogger(getClass)
