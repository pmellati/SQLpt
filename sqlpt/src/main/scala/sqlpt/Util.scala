package sqlpt

import java.util.UUID.randomUUID

import sqlpt.column._
import Column._
import sqlpt.ast.expressions.{Selection, Table, TablePartitioning}
import sqlpt.ast.statements.Statements._
import sqlpt.ast.statements.StringStatement
import sqlpt.ast.statements.Insertion._

import scala.reflect.runtime.universe.TypeTag

object Util {
//  def withTempTable[Cols <: Product, R](selection: Selection[Cols])(action: (=> Table[Cols]) => Statements): Statements = {
//    val uniqueTempTableName =
//      "sqlpt_temp_table_" + randomUUID
//
//    statements(
//      StringStatement(s"""
//        |CREATE TALBE $uniqueTempTableName
//        |ROW FORMAT DELIMITED FIELDS
//        |TERMINATED BY '|'
//        |STORED AS TEXTFILE
//      """.stripMargin),
//
//      insert(selection).into(uniqueTempTableName),
//
//      action(Table(uniqueTempTableName, selection.cols)),
//
//      StringStatement(s"""
//        |DROP TABLE IF EXISTS $uniqueTempTableName
//      """.stripMargin)
//    )
//  }

  trait TableDef {
    type Columns      <: Product
    type Partitioning <: TablePartitioning

    def name:         String
    def cols:         SourceColumn.InstantiationPermission => Columns
    def partitioning: Partitioning

    final def table = Table(name, cols(new SourceColumn.InstantiationPermission), partitioning)

    protected type Column[T <: Type]       = SourceColumn[T]
    protected type PartitionKey[T <: Type] = PartitionCol[T]

    protected implicit def str2Column[T <: Type : TypeTag]
    (colName: String)
    (implicit p: SourceColumn.InstantiationPermission): Column[T] = SourceColumn[T](name, colName)
  }
}
