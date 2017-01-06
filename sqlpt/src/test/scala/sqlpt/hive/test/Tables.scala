package sqlpt.hive.test

import hivevalid.HiveValid
import sqlpt.api._
import sqlpt.column.Column, Column._
import scala.reflect.runtime.universe._
import sqlpt.ast.expressions.Table.Partitioning

object Tables {
  object Cars extends TableDef with NoPartitioning {
    override def name = "db.cars"

    case class Columns(
      @ColumnName("car_id")
      id:      Column[Str],
      model:   Column[Str],
      @ColumnName("manufacturer_id")
      make:    Column[Str],
      price:   Column[Num],
      website: Column[Nullable[Str]]
    )
  }

  object CarMakers extends TableDef with NoPartitioning {
    override def name = "db.car_makers"

    case class Columns(
      @ColumnName("manufacturer_id")
      id:           Column[Str],
      @ColumnName("num_employees")
      numEmployees: Column[Num],
      hq:           Column[Str]
    )
  }

  trait TablesEnv {
    HiveValid.runMeta("""CREATE DATABASE IF NOT EXISTS db""")

    Seq(
      Cars.table,
      CarMakers.table
    ) foreach TableCreator.createTable
  }
}

object TableCreator {
  def createTable(table: Table[_ <: Product, _ <: Partitioning]): Unit = {
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
