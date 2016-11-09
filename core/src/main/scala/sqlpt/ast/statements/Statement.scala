package sqlpt.ast.statements

import sqlpt.ast.expressions.Selection

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
  (tableName: String, mode: Mode, partition: Seq[(String, String)], selection: Selection[_ <: Product])
  extends Statement {
    def inPartition(entries: (String, String)*) =
      copy(partition = entries)
  }

  def insert(selection: Selection[_ <: Product]) = new {
    def into(tableName: String) =
      Insertion(tableName, Mode.Into, Seq.empty, selection)

    def overwriteTable(tableName: String, ifNotExists: Boolean = false) =
      Insertion(tableName, Mode.OverwriteTable(ifNotExists), Seq.empty, selection)
  }
}