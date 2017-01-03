package sqlpt.ast.statements

import sqlpt.ast.expressions.{Selection, Table}

/** Signifies side-effecting statements that return unit. */
sealed trait Statement

case class StringStatement(sql: String) extends Statement

case class Insertion
(tableName: String, mode: Insertion.Mode, partition: Option[Product], selection: Selection[_ <: Product])
  extends Statement {
  def overwrite(ifNotExists: Boolean = false) =
    copy(mode = Insertion.Mode.OverwriteTable(ifNotExists))
}

object Insertion {
  sealed trait Mode
  object Mode {
    case object Into extends Mode
    case class OverwriteTable(ifNotExists: Boolean) extends Mode
  }

  trait Implicits {
    implicit class UnpartitionedTableInsertOps[Cols <: Product]
    (table: Table[Cols, Table.Partitioning.Unpartitioned]) {
      def insert(selection: Selection[Cols]) =
        Insertion(table.name, Mode.Into, None, selection)
    }

    implicit class PartitionedTableInsertOps[Cols <: Product, PartitionCols <: Product]
    (table: Table[Cols, Table.Partitioning.Partitioned[PartitionCols]]) {
      def insert(selection: Selection[Cols]) = new {
        def intoPartition(partitionSelection: Cols => PartitionCols) =
          Insertion(table.name, Mode.Into, Some(partitionSelection(selection.cols)), selection)
      }
    }
  }
}