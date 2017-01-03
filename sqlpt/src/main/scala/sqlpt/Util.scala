package sqlpt

import java.util.UUID.randomUUID

import sqlpt.column._
import Column._
import sqlpt.ast.expressions.{Selection, Table}, Table.Partitioning.Unpartitioned
import sqlpt.ast.statements.Statements._
import sqlpt.ast.statements.StringStatement
import sqlpt.ast.statements.Insertion

import scala.reflect.runtime.universe.TypeTag

object Util extends Insertion.Implicits {
  def withTempTable[Cols <: Product, R]
  (selection: Selection[Cols])(action: (=> Table[Cols, Unpartitioned]) => Statements): Statements = {
    val uniqueTempTableName =
      "sqlpt_temp_table_" + randomUUID

    val tempTable = Table(uniqueTempTableName, selection.cols, Unpartitioned)

    statements(
      // TODO: There may be existing code in tests to generate table DDL from `Table` instances.
      StringStatement(s"""
        |CREATE TALBE $uniqueTempTableName
        |ROW FORMAT DELIMITED FIELDS
        |TERMINATED BY '|'
        |STORED AS TEXTFILE
      """.stripMargin),

      tempTable.insert(selection),

      action(tempTable),

      StringStatement(s"""
        |DROP TABLE IF EXISTS $uniqueTempTableName
      """.stripMargin)
    )
  }

  trait TableDef {
    type Columns      <: Product
    type Partitioning <: Table.Partitioning

    def name:         String
    def cols:         SourceColumn.InstantiationPermission => Columns
    def partitioning: Partitioning

    final def table = Table(name, cols(new SourceColumn.InstantiationPermission), partitioning)

    protected implicit def str2Column[T <: Type : TypeTag]
    (colName: String)
    (implicit p: SourceColumn.InstantiationPermission): Column[T] = SourceColumn[T](name, colName)

    protected def col[T <: Type : TypeTag]
    (colName: String)
    (implicit p: SourceColumn.InstantiationPermission): Column[T] = str2Column(colName)
  }

  trait PartitioningDef {
    protected type PartitionKey[T <: Type] = PartitionCol[T]

    protected implicit def str2PartitionKey[T <: Type : TypeTag](keyName: String): PartitionCol[T] =
      PartitionCol.Key(keyName)
  }

  trait NoPartitioning extends PartitioningDef {this: TableDef =>
    override type Partitioning = Table.Partitioning.Unpartitioned
    override def partitioning = Table.Partitioning.Unpartitioned
  }
}
