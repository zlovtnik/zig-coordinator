package com.sslproxy.coordinator.sink

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.traverse.*
import doobie.*
import doobie.implicits.*
import com.sslproxy.coordinator.observability.StructuredLogger

import scala.concurrent.duration.*

final class SchemaIntrospector private (xa: Transactor[IO], database: String, refreshInterval: FiniteDuration, metaCache: Ref[IO, Map[String, TableMeta]]):
  import SchemaIntrospector.log

  def loadTableMeta(tableName: String): IO[TableMeta] =
    val sql =
      sql"""SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY, IS_NULLABLE, EXTRA
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = $database AND TABLE_NAME = $tableName
            ORDER BY ORDINAL_POSITION"""

    sql
      .query[(String, String, String, String, String)]
      .to[Vector]
      .transact(xa)
      .flatMap { rows =>
        if rows.isEmpty then
          IO.raiseError(IllegalArgumentException(s"table '$tableName' not found in database '$database'"))
        else
          val columns = rows.map { case (name, dataType, colKey, nullable, extra) =>
            ColumnMeta(
              name = name,
              dataType = dataType,
              isPk = colKey.equalsIgnoreCase("PRI"),
              isNullable = nullable.equalsIgnoreCase("YES"),
              isAutoInc = extra.toLowerCase(java.util.Locale.ROOT).contains("auto_increment")
            )
          }
          IO.pure(TableMeta(tableName = tableName.toLowerCase(java.util.Locale.ROOT), columns = columns))
      }

  def loadAll(tableNames: List[String]): IO[Map[String, TableMeta]] =
    tableNames
      .traverse { name =>
        loadTableMeta(name).map(m => Some(name -> m)).handleError { err =>
          log.error("schema_introspect", "status" -> "failed",
            "table" -> name, "error" -> err.getMessage)
          None
        }
      }
      .map(_.flatten.toMap)
      .flatTap(cache => metaCache.set(cache))

  def getMeta(tableName: String): IO[Option[TableMeta]] =
    metaCache.get.map(_.get(tableName.toLowerCase(java.util.Locale.ROOT)))

  def startRefresher(tableNames: List[String]): fs2.Stream[IO, Unit] =
    fs2.Stream
      .awakeEvery[IO](refreshInterval)
      .evalMap { _ =>
        loadAll(tableNames).void
      }

object SchemaIntrospector:
  private val log = StructuredLogger(getClass)

  def apply(xa: Transactor[IO], database: String, refreshInterval: FiniteDuration): IO[SchemaIntrospector] =
    Ref.of[IO, Map[String, TableMeta]](Map.empty).map { ref =>
      new SchemaIntrospector(xa, database, refreshInterval, ref)
    }
