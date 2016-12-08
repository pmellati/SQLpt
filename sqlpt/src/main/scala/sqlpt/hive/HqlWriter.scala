package sqlpt.hive

import sqlpt._, column._, Column._, Type._, Arithmetic._, Literals._, ast._, expressions._
import ColumnAffinitiesExtraction._
import scalaz._, Scalaz._

// TODO: Reconsider the names and put into separate file.
object ColumnAffinitiesExtraction {
  type Affinity      = Int
  type Affinities    = Seq[(SourceColumn[_ <: Type], Affinity)]    // TODO: Should be a Map that uses reference equality as equality for keys.
  type JoinCondition = Column[Bool]

  def affinitiesOf(rows: Rows[_ <: Product]): Affinities =
    sourcesOf(rows).map(_._1).zipWithIndex.foldLeft(Seq.empty: Affinities) {case (affinities, (src, affinity)) =>
      affinities ++ sourceColumnsWithAffinity(src, affinity)
    }

  def sourcesOf(rows: Rows[_ <: Product]): Seq[(Rows[_ <: Product], Option[JoinCondition])] = rows match {   // TODO: Instead return a Set that uses reference equality.
    case joined: BaseJoined =>
      joined.sourceSeq zip (None +: joined.ons.map(Some(_)))

    case nonJoin =>
      Seq(nonJoin -> None)
  }

  protected def sourceColumnsWithAffinity(rows: Rows[_ <: Product], affinity: Affinity): Affinities =
    sourceColumnsIn(rows.cols).map(_ -> affinity)

  protected def sourceColumnsIn(cols: Product): Seq[SourceColumn[_ <: Type]] = cols match {
    case column: Column[_] =>
      sourceColumnsInColumn(column)
    case tupleOrCaseClass: Product =>
      tupleOrCaseClass.productIterator.flatMap {col =>
        sourceColumnsIn(col.asInstanceOf[Product])
      }.toSeq
  }

  protected def sourceColumnsInColumn(column: Column[_ <: Type]): Seq[SourceColumn[_ <: Type]] = column match {
    case sourceColumn: SourceColumn[_] =>
      Seq(sourceColumn)
    case Equals(left, right) =>
      sourceColumnsInColumn(left) ++ sourceColumnsIn(right)
    case _ =>
      ???   // TODO
  }
}