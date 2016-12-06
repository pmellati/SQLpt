package sqlpt.hive

import sqlpt._, column._, Column._, Type._, Arithmetic._, Literals._, ast._, expressions._
import ColumnAffinitiesExtraction._
import scalaz._, Scalaz._

object SelectionTranslator extends Translator[Selection[_ <: Product]] with ColumnImplicits {
  override def apply(selection: Selection[_ <: Product]): Hql = selection match {
    case ss: SimpleSelection[_, _] =>
      toHql(ss)
    case _ =>
      ???
  }

  def toHql(selection: SimpleSelection[_ <: Product, _ <: Product]): Hql = {
    val affinities = affinitiesOf(selection.source)

    val selectedColumns = selection.cols match {
      case singleColumn: Column[_] =>
        toHql(singleColumn, affinities)
      case product =>
        product.productIterator.map {component =>
          toHql(component.asInstanceOf[Column[_]], affinities)   // TODO: We may also get a product (tuple or case class) here.
        }.mkString(", ")
    }

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
