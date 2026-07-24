package com.sslproxy.coordinator.sink

import cats.effect.IO
import com.sslproxy.coordinator.tidb.TidbErrorClass
import doobie.*
import doobie.implicits.*
import doobie.free.connection.raw
import com.sslproxy.coordinator.observability.StructuredLogger

import java.sql.{PreparedStatement, Types}
import scala.concurrent.duration.*

class GenericTiDbSink(xa: Transactor[IO]):
  import GenericTiDbSink.log

  private val maxRetries = 3
  private val baseDelay: FiniteDuration = 200.millis

  def upsert(tableMeta: TableMeta, row: Row): IO[Int] =
    upsertWithRetry(tableMeta, row, 1)

  def batchUpsert(tableMeta: TableMeta, rows: List[Row]): IO[Int] =
    batchUpsertWithRetry(tableMeta, rows, 1)

  private def upsertWithRetry(tableMeta: TableMeta, row: Row, attempt: Int): IO[Int] =
    val sql = buildUpsertSql(tableMeta)
    val normalized = tableMeta.insertColumns.map { c =>
      val key = c.name.toLowerCase(java.util.Locale.ROOT)
      row.getOrElse(key, ColValue.VNull)
    }

    val program: ConnectionIO[Int] = raw { conn =>
      val ps = conn.prepareStatement(sql)
      try
        normalized.zipWithIndex.foreach { case (v, idx) =>
          setColValue(ps, idx + 1, v)
        }
        ps.executeUpdate()
      finally ps.close()
    }

    program.transact(xa).handleErrorWith { err =>
      if attempt < maxRetries && isRetryable(err) then
        val delay = baseDelay * (1L << (attempt - 1))
        log.warn("generic_sink_retry", "status" -> "retrying",
          "table" -> tableMeta.tableName, "attempt" -> s"$attempt/$maxRetries",
          "delay" -> s"${delay.toMillis}ms", "error" -> err.getMessage)
        IO.sleep(delay) *> upsertWithRetry(tableMeta, row, attempt + 1)
      else
        IO.raiseError(err)
    }

  private def batchUpsertWithRetry(tableMeta: TableMeta, rows: List[Row], attempt: Int): IO[Int] =
    if rows.isEmpty then IO.pure(0)
    else
      val sql = buildUpsertSql(tableMeta)
      val normalized = rows.map { row =>
        tableMeta.insertColumns.map { c =>
          val key = c.name.toLowerCase(java.util.Locale.ROOT)
          row.getOrElse(key, ColValue.VNull)
        }
      }

      val program: ConnectionIO[Int] = raw { conn =>
        val ps = conn.prepareStatement(sql)
        try
          for rowValues <- normalized do
            rowValues.zipWithIndex.foreach { case (v, idx) =>
              setColValue(ps, idx + 1, v)
            }
            ps.addBatch()
          ps.executeBatch().sum
        finally ps.close()
      }

      program.transact(xa).handleErrorWith { err =>
        if attempt < maxRetries && isRetryable(err) then
          val delay = baseDelay * (1L << (attempt - 1))
          log.warn("generic_sink_batch_retry", "status" -> "retrying",
            "table" -> tableMeta.tableName, "attempt" -> s"$attempt/$maxRetries",
            "rows" -> rows.size.toString, "delay" -> s"${delay.toMillis}ms",
            "error" -> err.getMessage)
          IO.sleep(delay) *> batchUpsertWithRetry(tableMeta, rows, attempt + 1)
        else
          IO.raiseError(err)
      }

  def buildUpsertSql(tableMeta: TableMeta): String =
    val cols = tableMeta.insertColumns
    val colNames = cols.map(_.quotedName).mkString(", ")
    val placeholders = cols.map(_ => "?").mkString(", ")
    val updates = cols.map { c =>
      if c.isPk then ""
      else s"${c.quotedName} = VALUES(${c.quotedName})"
    }.filter(_.nonEmpty).mkString(", ")

    if updates.isEmpty then
      s"INSERT INTO ${tableMeta.quotedTable} ($colNames) VALUES ($placeholders)"
    else
      s"INSERT INTO ${tableMeta.quotedTable} ($colNames) VALUES ($placeholders) ON DUPLICATE KEY UPDATE $updates"

  private def setColValue(ps: PreparedStatement, idx: Int, value: ColValue): Unit =
    value match
      case ColValue.VNull        => ps.setNull(idx, Types.NULL)
      case ColValue.VStr(s)      => ps.setString(idx, s)
      case ColValue.VLong(n)     => ps.setLong(idx, n)
      case ColValue.VDecimal(bd) => ps.setBigDecimal(idx, bd.bigDecimal)
      case ColValue.VDouble(d)   => ps.setDouble(idx, d)
      case ColValue.VBool(b)     => ps.setBoolean(idx, b)
      case ColValue.VJson(j)     => ps.setString(idx, j)

  private def isRetryable(t: Throwable): Boolean =
    TidbErrorClass.classify(t) == TidbErrorClass.Retryable

object GenericTiDbSink:
  private val log = StructuredLogger(getClass)
