package sqlpt

import ast.{expressions => expr}
import ast.{statements  => stmt}

package object api extends column.ColumnImplicits {
  type Selection[Cols <: Product] = expr.Selection[Cols]

  type Table[Cols <: Product, Partitioning <: expr.TablePartitioning] = expr.Table[Cols, Partitioning]

  implicit def rows2Filtered[Src <: Product](rows: expr.Rows[Src]): expr.Filtered[Src] =
    expr.Filtered(rows, Set.empty)

  type Statement       = stmt.Statement
  type StringStatement = stmt.StringStatement
  val  StringStatement = stmt.StringStatement

  type Statements = stmt.Statements.Statements
  def statements(s: Statements, ss: Statements*) =
    stmt.Statements.statements(s, ss: _*)

  implicit def statementToStatements(statement: Statement): Statements =
    stmt.Statements.statementToStatements(statement)

  type Insertion = stmt.Insertion.Insertion

  def withTempTable[Cols <: Product, R](selection: Selection[Cols])(action: (=> Table[Cols, expr.TablePartitioning.Unpartitioned]) => Statements) = ???
//    Util.withTempTable[Cols, R](selection)(action)

  type TableDef = Util.TableDef

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

  implicit def typeCheck[T]: Columns[T] =
    macro ColumnsTypeChecking.typeCheck_impl[T]
}
