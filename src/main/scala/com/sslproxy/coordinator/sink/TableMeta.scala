package com.sslproxy.coordinator.sink

type Row = Map[String, ColValue]

final case class ColumnMeta(
    name: String,
    dataType: String,
    isPk: Boolean,
    isNullable: Boolean,
    isAutoInc: Boolean
):
  def quotedName: String = s"`$name`"

final case class TableMeta(
    tableName: String,
    columns: Vector[ColumnMeta]
):
  val pkColumns: Vector[ColumnMeta] = columns.filter(_.isPk)
  val nonPkColumns: Vector[ColumnMeta] = columns.filter(c => !c.isPk && !c.isAutoInc)
  val insertColumns: Vector[ColumnMeta] = columns.filterNot(_.isAutoInc)
  val quotedTable: String = s"`$tableName`"
