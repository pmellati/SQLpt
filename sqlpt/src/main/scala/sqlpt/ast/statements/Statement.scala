package sqlpt.ast.statements

import sqlpt.ast.expressions.{Selection, Table}

/** Signifies side-effecting statements that return unit. */
sealed trait Statement

case class StringStatement(sql: String) extends Statement

case class Insertion
  (outputTable: Table[_ <: Product], mode: Insertion.Mode, selection: Selection[_ <: Product])
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
    (table: Table[Cols]) {
      def insert(selection: Selection[Cols]) =
        Insertion(table, Mode.Into, selection)
    }
  }
}