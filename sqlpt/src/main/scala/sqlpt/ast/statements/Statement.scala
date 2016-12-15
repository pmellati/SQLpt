package sqlpt.ast.statements

import sqlpt.ast.expressions.{Selection, Table, TablePartitioning}

/** Signifies side-effecting statements that return unit. */
sealed trait Statement

case class StringStatement(sql: String) extends Statement

object Insertion {
  sealed trait Mode
  object Mode {
    case object Into extends Mode
    case class OverwriteTable(ifNotExists: Boolean) extends Mode
  }

  case class Insertion
  (tableName: String, mode: Mode, partition: Option[Product], selection: Selection[_ <: Product])
  extends Statement {
    def overwrite(ifNotExists: Boolean = false) =
      copy(mode = Mode.OverwriteTable(ifNotExists))
  }

  implicit class UnpartitionedTableInsertOps[Cols <: Product]
  (table: Table[Cols, TablePartitioning.Unpartitioned.type]) {
    def insert(selection: Selection[Cols]) =
      Insertion(table.name, Mode.Into, None, selection)
  }

  implicit class PartitionedTableInsertOps[Cols <: Product, PartitionCols <: Product]
  (table: Table[Cols, TablePartitioning.Partitioned[PartitionCols]]) {
    def insert(selection: Selection[Cols]) = new {
      def intoPartition(partitionSelection: Cols => PartitionCols) =
        Insertion(table.name, Mode.Into, Some(partitionSelection(selection.cols)), selection)
    }
  }

  object Usage {
    import sqlpt.api.{Table => _, _}
    import sqlpt.column.{PartitionCol, PartitionKey, PartitionVal}
    import scala.reflect.runtime.universe.TypeTag

    object Employees extends TableDef with PartitioningByDate {
      override def name = "mydb.employees"

      case class Columns(
        id:   Column[Str],
        name: Column[Str],
        age:  Column[Num]
      )

      override def cols = implicit permission =>
        Columns(
          id   = "employee_id",
          name = "name",
          age  = "age"
        )
    }

    object Games extends TableDef with NoPartitioning {
      override def name = "games"

      case class Columns(
        name:  Column[Str],
        genre: Column[Str]
      )

      override def cols = implicit permission =>
        Columns(
          name  = "name",
          genre = "genre"
        )
    }

    trait PartitioningByDate extends PartitioningDef {this: TableDef =>
      case class Partition(
        year:  PartitionKey[Num],
        month: PartitionKey[Num],
        day:   PartitionKey[Num]
      )

      override type Partitioning = TablePartitioning.Partitioned[Partition]

      override def partitioning = TablePartitioning.Partitioned(
        Partition(
          year  = "year",
          month = "month",
          day   = "day"
        )
      )
    }

    trait NoPartitioning extends PartitioningDef {this: TableDef =>
      override type Partitioning = TablePartitioning.Unpartitioned
      override def partitioning = TablePartitioning.Unpartitioned
    }

    trait PartitioningDef {
      protected implicit def str2PartitionKey[T <: Type : TypeTag](keyName: String): PartitionKey[T] =
        PartitionKey(keyName)
    }

    Employees.table.insert(
      Employees.table.select {employee =>
        Employees.Columns(
          id   = employee.id,
          name = employee.name,
          age  = employee.age
        )
      }
    ).intoPartition(employee => Employees.Partition(
      year  = employee.age,
      month = literal(1),
      day   = literal(22)
    ))

    Games.table.insert(
      Games.table.select(identity)
    ).overwrite()

    implicit def col2PartionCol[T <: Column.Type : TypeTag](col: Column[T]): PartitionCol[T] =
      PartitionVal(col)
  }
}