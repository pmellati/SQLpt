package sqlpt.hive

import sqlpt._, Column._, Type._, Arithmetic._, Literal._
import ColumnAffinitiesExtraction._
import scalaz._, Scalaz._

object HqlWriter {
  type Hql = String

  def toHql(selection: SimpleSelection[_ <: Product, _ <: Product]): Hql = {
    val affinities = affinitiesOf(selection.source)

    val selectedColumns =
      selection.cols.productIterator
        .map {subProduct =>
          toHql(subProduct.asInstanceOf[Column[_]], affinities)   // TODO: We may also get a product (tuple or case class) here.
        }.mkString(", ")

    val optionallyDistinct = selection.isDistinct ? "DISTINCT" | ""

    val sources = sourcesOf(selection.source)

    val fromClause = sources.zipWithIndex.map {case ((source, onCondition), index) =>
      val fromOrJoinType = (index == 0) ? "FROM" | "JOIN"  // TODO: Can't always assume inner join.

      val (optParenOpen, optParenClose) = !source.isInstanceOf[Table[_]] ? ("(", ")") | ("", "")

      val optAlias =
        if (sources.length > 1 || !source.isInstanceOf[Table[_]])
          affinityToLetter(index)
        else
          ""

      val optOn = onCondition.fold("") {onCondition => s"ON ${toHql(onCondition, affinities)}"}

      s"$fromOrJoinType $optParenOpen ${Internal.toHql(source)} $optParenClose $optAlias $optOn"
    }.mkString("\n")

    val whereClause = {
      val allFilters = selection.filters.reduceOption(_ and _)
      allFilters.fold("") {allFilters =>
        s"WHERE ${toHql(allFilters, affinities)}"
      }
    }

    // TODO: Here we should also decide whether the source requires parens and / or aliases.
    s"""SELECT $optionallyDistinct $selectedColumns
        |$fromClause
        |$whereClause
      """.stripMargin
  }

  def toHql(table: Table[_]) =
    table.name

  private object Internal {
    def toHql(rows: Rows[_ <: Product]): Hql = rows match {
      case selection: SimpleSelection[_, _] =>
        HqlWriter.toHql(selection)
      case table: Table[_] =>
        HqlWriter.toHql(table)
      case xx =>
        ???
    }
  }


  def toHql(column: Column[_], affinity: Affinities): Hql = column match {
    case sourceColumn: SourceColumn[_] =>
      s"${affinityToLetter(affinity.find{_._1 eq sourceColumn}.get._2)}.${sourceColumn.name}"

    case Equals(left, right) =>
      s"${toHql(left, affinity)} = ${toHql(right, affinity)}"

    case Multiplication(left, right) =>
      s"${toHql(left, affinity)} * ${toHql(right, affinity)}"

    case LiteralNum(n) =>
      n.toString

    case unimplemented =>
      throw new NotImplementedError(s"Not Implemented: toHql($unimplemented)")
  }

  private def affinityToLetter(affinity: Affinity): String =
    ('A' to 'Z').map(_.toString).toSeq(affinity)
}

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
    case InnerJoin(left, right, onCondition) =>
      if (right.isInstanceOf[InnerJoin[_,_]]) throw new IllegalStateException

      sourcesOf(left) :+ right -> Some(onCondition)
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