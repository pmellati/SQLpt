package sqlpt.hive

import sqlpt.ast.expressions.{BaseJoined, JoinMode, Rows}
import sqlpt.column._, Column._, Type._, Literals._, Arithmetic._

object JoinTranslationHelpers {
  type Affinity      = Int
  type Affinities    = Seq[(SourceColumn[_ <: Type], Affinity)]    // TODO: Should be a Map that uses reference equality as equality for keys.
  type JoinCondition = Column[Bool]

  def affinitiesOf(rows: Rows[_ <: Product]): Affinities =
    sourcesOfJoin(rows).zipWithIndex.foldLeft(Seq.empty: Affinities) {case (affinities, (src, affinity)) =>
      affinities ++ sourceColumnsWithAffinity(src, affinity)
    }

  def joinInfo(joined: BaseJoined): (Rows[_ <: Product], Seq[(Rows[_ <: Product], JoinCondition, JoinMode)]) =
    joined.sourceSeq.head ->
      joined.sourceSeq.tail.zip(joined.ons).zip(joined.joinModes).map {case ((src, cond), mode) =>
        (src, cond, mode)
      }


  def sourcesOfJoin(rows: Rows[_ <: Product]): Seq[Rows[_ <: Product]] = rows match {
    case joined: BaseJoined =>
      joined.sourceSeq

    case nonJoin =>
      Seq(nonJoin)
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
    case LiteralStr(_) | LiteralNum(_) =>
      Seq.empty
    case other => // TODO
      sys.error(s"Not implemented: $other")
  }
}