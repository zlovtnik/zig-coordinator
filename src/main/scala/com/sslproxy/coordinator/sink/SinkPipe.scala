package com.sslproxy.coordinator.sink

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.errors.DeadLetter
import com.sslproxy.coordinator.model.SystemContext
import fs2.Pipe
import org.slf4j.LoggerFactory

final class SinkPipe(
    sink: GenericTiDbSink,
    meta: SchemaIntrospector,
    registry: SystemRegistry,
    dlqSink: DeadLetter => IO[Unit]
):
  import SinkPipe.log

  val pipe: Pipe[IO, (String, SystemContext, Row), Unit] =
    _.evalMap { case (table, ctx, row) =>
      val fullRow = row ++ ctx.asColumns
      (for
        _        <- registry.validate(ctx)
        tableMeta <- meta.getMeta(table).flatMap {
          case Some(m) => IO.pure(m)
          case None =>
            val err = s"unknown table '$table'"
            log.warn("event=sink_pipe status=unknown_table table={}", table)
            IO.raiseError(IllegalArgumentException(err))
        }
        _        <- logRow(table, ctx, row)
        _        <- sink.upsert(tableMeta, fullRow)
      yield ()).handleErrorWith { err =>
        val dl = DeadLetter(table = table, ctx = ctx, row = row, error = err.getMessage)
        log.error("event=sink_pipe status=dlq table={} origin={} error=\"{}\"", table, ctx.origin, sanitize(err.getMessage))
        dlqSink(dl)
      }
    }

  def processBatch(records: List[(String, SystemContext, Row)]): IO[Unit] =
    records.groupBy(_._1).toList.traverse_ { case (table, items) =>
      (for
        _         <- items.traverse_ { case (_, ctx, _) => registry.validate(ctx) }
        tableMeta <- meta.getMeta(table).flatMap {
          case Some(m) => IO.pure(m)
          case None    =>
            val err = s"unknown table '$table'"
            log.warn("event=sink_pipe_batch status=unknown_table table={}", table)
            IO.raiseError(IllegalArgumentException(err))
        }
        _         <- IO(log.debug("event=sink_pipe_batch status=write table={} count={}", table, items.size))
        rows = items.map { case (_, ctx, row) => row ++ ctx.asColumns }
        _    <- sink.batchUpsert(tableMeta, rows)
      yield ()).handleErrorWith { err =>
        log.error("event=sink_pipe_batch status=dlq table={} count={} error=\"{}\"",
          table, items.size, sanitize(err.getMessage))
        items.traverse_ { case (table, ctx, row) =>
          val dl = DeadLetter(table = table, ctx = ctx, row = row, error = err.getMessage)
          dlqSink(dl)
        }
      }
    }

  private def logRow(table: String, ctx: SystemContext, row: Row): IO[Unit] =
    IO(log.debug("event=sink_pipe status=write table={} origin={} cols={}", table, ctx.origin, row.size))

  private def sanitize(msg: String): String =
    if msg == null then "" else msg.replace('\n', ' ').replace('\r', ' ')

object SinkPipe:
  private val log = LoggerFactory.getLogger(getClass)
