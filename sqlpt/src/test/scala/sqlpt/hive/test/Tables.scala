package sqlpt.hive.test

import hivevalid.HiveValid
import sqlpt.api._
import sqlpt.column.Column, Column._
import scala.reflect.runtime.universe._

object Tables {
  object Cars extends TableDef {
    override def name = "db.cars"

    case class Columns(
      id:      Column[Str]           = "car_id",
      model:   Column[Str]           = "model",
      make:    Column[Str]           = "manufacturer_id",
      price:   Column[Num]           = "price",
      website: Column[Nullable[Str]] = "website"
    )

    override def cols = Columns()
  }

  object CarMakers extends TableDef {
    override def name = "db.car_makers"

    case class Columns(
      id:           Column[Str] = "manufacturer_id",
      numEmployees: Column[Num] = "num_employees",
      hq:           Column[Str] = "hq"
    )

    override def cols = Columns()
  }

  trait TablesEnv {
    HiveValid.runMeta("""CREATE DATABASE IF NOT EXISTS db""")

    Seq(
      Cars.table,
      CarMakers.table
    ) foreach createTable
  }

  private def createTable(table: Table[_ <: Product]): Unit = {
    val columnsDecls = table.cols.productIterator.toSeq.map {colAny =>
      val col = colAny.asInstanceOf[SourceColumn[_ <: Column.Type]]

      s"${col.name} ${hqlTypeNameOf(col)}"
    }

    HiveValid.runMeta(s"""
      |CREATE TABLE IF NOT EXISTS ${table.name} ( ${columnsDecls.mkString(", ")} )
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY '\t'
      |LINES TERMINATED BY '\n'
      |STORED AS TEXTFILE
    """.stripMargin)
  }

  private def hqlTypeNameOf(c: SourceColumn[_ <: Column.Type]): String = {
    def typeToSqlType(typee: reflect.runtime.universe.Type): String = {
      if (typee <:< typeOf[Column.Type.Str])
        "string"
      else if (typee <:< typeOf[Column.Type.Bool])
        "boolean"
      else if (typee <:< typeOf[Column.Type.Num])
        "double"
      else
        sys.error("Unsupported column type.")
    }

    if (c.columnTypeTag.tpe <:< typeOf[Column.Type.Nullable[_]])
      typeToSqlType(c.columnTypeTag.tpe.typeArgs.head)
    else
      typeToSqlType(c.columnTypeTag.tpe)
  }
}
