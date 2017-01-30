package sqlpt.hive.test

import hivevalid.HiveValid
import sqlpt.api._
import sqlpt.column.Column, Column._
import scala.reflect.runtime.universe._

object Tables {
  object Cars extends MyOrgTable {
    override def name = "db.cars"

    case class Columns(
      carId:   Column[Str],
      model:   Column[Str],
      make:    Column[Str],
      price:   Column[Num],
      website: Column[Nullable[Str]]
    )
  }

  object CarMakers extends MyOrgTable {
    override def name = "db.car_makers"

    case class Columns(
      manufacturerId: Column[Str],
      numEmployees:   Column[Num],
      hq:             Column[Str]
    )
  }

  trait MyOrgTable extends TableDef {
    override def fieldNameToColumnName = {fieldName: String =>
      "[A-Z]?[^A-Z]*".r.findAllIn(fieldName).toList.filterNot(_.isEmpty).map(_.toLowerCase).mkString("_")
    }
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
  def createTable(table: Table[_ <: Product]): Unit = {
    val allCols = table.cols.productIterator.toSeq.asInstanceOf[Seq[SourceColumn[_ <: Column.Type]]]

    def declarationStringOf(cols: Seq[SourceColumn[_ <: Column.Type]]) =
      cols.map {col => s"${col.name} ${hqlTypeNameOf(col)}"}.mkString(", ")

    val nonPartitioningColsDecl = declarationStringOf(allCols.filterNot(_.isPartitioning))
    val partitioningColsDecl    = declarationStringOf(allCols.filter(_.isPartitioning))

    HiveValid.runMeta(s"""
      |CREATE TABLE IF NOT EXISTS ${table.name} ( $nonPartitioningColsDecl )
      |${if (partitioningColsDecl.isEmpty) "" else s"""PARTITIONED BY ( $partitioningColsDecl )"""}
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
