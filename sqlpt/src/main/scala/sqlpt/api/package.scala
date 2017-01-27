package sqlpt

import ast.{expressions => expr}
import ast.{statements  => stmt}

package object api extends column.ColumnImplicits with stmt.Insertion.Implicits {
  type Selection[Cols <: Product] = expr.Selection[Cols]

  type Table[Cols <: Product] = expr.Table[Cols]
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
  (action: (=> Table[Cols]) => Statements) =
    util.Misc.withTempTable[Cols, R](selection)(action)

  type TableDef = util.TableDef
  type PartitioningColumn[+T <: column.Column.Type] = util.TableDef.PartitioningColumn[T]

  def partition[T <: column.Column.Type](c: Column[T]): Column[T] with util.TableDef.PartitioningColumnTag =
    c.asInstanceOf[Column[T] with util.TableDef.PartitioningColumnTag]

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

  implicit def typeCheck[T]: Columns[T] =
    macro ColumnsTypeChecking.typeCheck_impl[T]
}
