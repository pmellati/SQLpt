package sqlpt

import ast.{expressions => expr}
import ast.{statements  => stmt}

package object api extends column.ColumnImplicits with stmt.Insertion.Implicits {
  type Selection[Cols <: Product] = expr.Selection[Cols]

  type Table[Cols <: Product, Partitioning <: expr.Table.Partitioning] = expr.Table[Cols, Partitioning]
  val  Table = expr.Table

  type Statement       = stmt.Statement
  type StringStatement = stmt.StringStatement
  val  StringStatement = stmt.StringStatement

  type Statements = stmt.Statements.Statements
  def statements(s: Statements, ss: Statements*) =
    stmt.Statements.statements(s, ss: _*)

  type Insertion = stmt.Insertion

  def withTempTable[Cols <: Product, R]
  (selection: Selection[Cols])
  (action: (=> Table[Cols, expr.Table.Partitioning.Unpartitioned]) => Statements) =
    util.Misc.withTempTable[Cols, R](selection)(action)

  type TableDef        = util.TableDef
  type PartitioningDef = util.PartitioningDef
  type NoPartitioning  = util.NoPartitioning

  type Column[+T <: column.Column.Type] = column.Column[T]
  val  Column                           = column.Column

  type Type = Column.Type
  val  Type = Column.Type

  type Str  = Column.Type.Str
  type Num  = Column.Type.Num
  type Bool = Column.Type.Bool

  type Nullable[T <: column.Column.Type] = column.Column.Type.Nullable[T]

  def partitionBy(col1: Column[_ <: Type], cols: Column[_ <: Column.Type]*) =
    column.WindowingAndAnalytics.partitionBy(col1, cols: _*)

  val UnixTimestamp = column.Dates.UnixTimestamp
  val FromUnixTime  = column.Dates.FromUnixTime
  val ToDate        = column.Dates.ToDate
  val DateAdd       = column.Dates.DateAdd

  type Columns[T] = sqlpt.Columns[T]

  implicit def rows2Filtered[Src <: Product](rows: expr.Rows[Src]): expr.Filtered[Src] =
    expr.Filtered(rows, Set.empty)

  implicit def statementToStatements(statement: Statement): Statements =
    stmt.Statements.statementToStatements(statement)

//  This compiles but trips intellij, even if it is not defined as implicit!
//  implicit def col2PartionColVal[T <: Column.Type : TypeTag](col: Column[T]): column.PartitionCol[T] =
//    column.PartitionCol.Val(col)

  implicit def typeCheck[T]: Columns[T] =
    macro ColumnsTypeChecking.typeCheck_impl[T]
}
