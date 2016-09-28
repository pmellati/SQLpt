package sqlpt.hive

import sqlpt._, Column._, Arithmetic._
import ColumnAffinitiesExtraction._

object HqlWriter {
  type Hql = String

  def toHql(rows: Rows[_ <: Product]): Hql = rows match {
    case selection: Selection[_, _] =>
      val affinities = affinitiesOf(selection.source)

      println(s"Sources:\n${findSources(selection.source)}")
      println()
      println(s"Affinities:\n${affinitiesOf(selection.source)}")

      val selectedColumns =
        selection.cols.productIterator
        .map {subProduct =>
          toHql(subProduct.asInstanceOf[Column[_]], affinities)   // TODO: We may also get a product (tuple or case class) here.
        }.mkString(", ")
//
//      val whereClause = selection.filters.reduceOption {_ and _}.fold("") {filter =>
//        s"""WHERE ${toHql(filter)}"""
//      }
//
      val optionallyDistinct =
        if (selection.isDistinct) "DISTINCT" else ""



      // TODO: Here we should also decide whether the source requires parens and / or aliases.
      s"""SELECT $optionallyDistinct $selectedColumns
         |FROM ({toHql(selection.source)})
         |whereClause
      """.stripMargin

    case xx => ???
  }

  def toHql(column: Column[_], affinity: Affinities): Hql = column match {
    case sourceColumn: SourceColumn[_] =>
      s"${affinityToLetter(affinity.find{_._1 eq sourceColumn}.get._2)}.${sourceColumn.name}"

    case Equals(left, right) =>
      s"${toHql(left, affinity)} = ${toHql(right, affinity)}"

    case Multiplication(left, right) =>
      s"${toHql(left, affinity)} * ${toHql(right, affinity)}"

    case unimplemented =>
      throw new NotImplementedError(s"Not Implemented: toHql($unimplemented)")
  }

  private def affinityToLetter(affinity: Affinity): String =
    ('A' to 'Z').map(_.toString).toSeq(affinity)
}

// TODO: Reconsider the names and put into separate file.
object ColumnAffinitiesExtraction {
  type Affinity     = Int
  type NextAffinity = Int
  type Affinities   = Seq[(SourceColumn[_ <: Type], Affinity)]    // TODO: Should be a Map that uses reference equality as equality for keys.

  def affinitiesOf(rows: Rows[_ <: Product]): Affinities =
    findSources(rows).zipWithIndex.foldLeft(Seq.empty: Affinities) {case (affinities, (src, affinity)) =>
      affinities ++ sourceColumnsWithAffinity(src, affinity)
    }

  def findSources(rows: Rows[_ <: Product]): Seq[Rows[_ <: Product]] = rows match {   // TODO: Instead return a Set that uses reference equality.
    case InnerJoin(left, right, _) =>
      findSources(left) ++ findSources(right)
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
    case _ =>
      ???   // TODO
  }
}