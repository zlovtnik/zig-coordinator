package com.sslproxy.coordinator.sink

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.errors.DeadLetter
import com.sslproxy.coordinator.model.SystemContext
import fs2.Pipe
import com.sslproxy.coordinator.observability.StructuredLogger

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
            log.warn("sink_pipe", "status" -> "unknown_table", "table" -> table)
            IO.raiseError(IllegalArgumentException(err))
        }
        _        <- logRow(table, ctx, row)
        _        <- sink.upsert(tableMeta, fullRow)
      yield ()).handleErrorWith { err =>
        val dl = DeadLetter(table = table, ctx = ctx, row = row, error = err.getMessage)
        log.error("sink_pipe", "status" -> "dlq",
          "table" -> table, "origin" -> ctx.origin, "error" -> err.getMessage)
        dlqSink(dl)
      }
    }

  def processBatch(records: List[(String, SystemContext, Row)]): IO[Unit] =
    records.groupBy(_._1).toList.traverse_ { case (table, items) =>
      (for
        validated <- items.traverse { case (_, ctx, row) =>
          registry.validate(ctx).as((ctx, row)).attempt.map {
            case Right(v)         => Right(v)
            case Left(err)        => Left((ctx, row, err.getMessage))
          }
        }
        invalid = validated.collect { case Left(info) => info }
        valid   = validated.collect { case Right(v)   => v }
        _ <- invalid.traverse_ { case (ctx, row, err) =>
          val dl = DeadLetter(table = table, ctx = ctx, row = row, error = err)
          log.warn("sink_pipe_batch", "status" -> "validation_failed",
            "table" -> table, "origin" -> ctx.origin, "error" -> err)
          dlqSink(dl)
        }
        tableMeta <- meta.getMeta(table).flatMap {
          case Some(m) => IO.pure(m)
          case None    =>
            val err = s"unknown table '$table'"
            log.warn("sink_pipe_batch", "status" -> "unknown_table", "table" -> table)
            IO.raiseError(IllegalArgumentException(err))
        }
        _ <- IO.whenA(valid.nonEmpty)(
          IO(log.debug("sink_pipe_batch", "status" -> "write",
            "table" -> table, "valid" -> valid.size.toString, "invalid" -> invalid.size.toString))
        )
        _ <- IO.whenA(valid.nonEmpty) {
          val rows = valid.map { case (ctx, row) => row ++ ctx.asColumns }
          sink.batchUpsert(tableMeta, rows).void
        }
      yield ()).handleErrorWith { err =>
        log.error("sink_pipe_batch", "status" -> "dlq",
          "table" -> table, "count" -> items.size.toString, "error" -> err.getMessage)
        items.traverse_ { case (table, ctx, row) =>
          val dl = DeadLetter(table = table, ctx = ctx, row = row, error = err.getMessage)
          dlqSink(dl)
        }
      }
    }

  private def logRow(table: String, ctx: SystemContext, row: Row): IO[Unit] =
    IO(log.debug("sink_pipe", "status" -> "write",
      "table" -> table, "origin" -> ctx.origin, "cols" -> row.size.toString))

object SinkPipe:
  private val log = StructuredLogger(getClass)
