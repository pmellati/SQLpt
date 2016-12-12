package sqlpt.hive

import sqlpt._, column._, Column._, Type._, Arithmetic._, Literals._, ast._, expressions._
import JoinTranslationHelpers._
import scalaz._, Scalaz._

object Translators extends ColumnImplicits {
  def selection: Translator[Selection[_ <: Product]] = {
    case ss: SimpleSelection[_, _] =>
      simpleSelection(ss)
    case _ =>
      ???
  }

  def simpleSelection: Translator[SimpleSelection[_ <: Product, _ <: Product]] = {selection =>
    val affinities = affinitiesOf(selection.source)

    val selectedColumns = columnsProduct(selection.cols, affinities)

    val optionallyDistinct = selection.isDistinct ? "DISTINCT" | ""

    val fromClause = {
      def src2Str(src: Rows[_ <: Product]): String = {
        val needsParentheses =
          !src.isInstanceOf[Table[_]] && !src.isInstanceOf[Outer[_]]

        val (optParenOpen, optParenClose) = needsParentheses ? ("(", ")") | ("", "")

        s"$optParenOpen ${rows(src)} $optParenClose"
      }

      selection.source match {
        case joined: BaseJoined =>
          val (firstSource, rest) = joinInfo(joined)

          val firstLine = s"""FROM ${src2Str(firstSource)} A"""

          val restOfLines = rest.zipWithIndex map {case ((src, joinCond, joinMode), index) =>
            val joinModeStr = joinMode match {
              case JoinMode.Inner => "JOIN"
              case JoinMode.Left  => "LEFT JOIN"
              case JoinMode.Right => "RIGHT JOIN"
            }

            val alias = affinityToLetter(index + 1)

            val onClause = s"ON ${singleColumn(joinCond, affinities)}"

            s"""$joinModeStr ${src2Str(src)} $alias $onClause"""
          }

          (firstLine +: restOfLines) mkString "\n"

        case nonJoined =>
          s"""FROM ${src2Str(nonJoined)} A"""
      }
    }

    val whereClause = {
      val allFilters = selection.filters.reduceOption(_ and _)
      allFilters.fold("") {allFilters =>
        s"WHERE ${singleColumn(allFilters, affinities)}"
      }
    }

    s"""SELECT $optionallyDistinct $selectedColumns
      |$fromClause
      |$whereClause
    """.stripMargin
  }

  private def table: Translator[Table[_]] = _.name

  private def rows: Translator[Rows[_ <: Product]] = {
    case selection: SimpleSelection[_, _] =>
      simpleSelection(selection)
    case t: Table[_] =>
      table(t)
    case Outer(r) =>
      rows(r)
    case unimplemented =>
      throw new NotImplementedError(s"Not Implemented: Translators.rows($unimplemented)")
  }

  private def columnsProduct(cols: Product, affinities: Affinities): Hql = {
    def columnHqls(cols: Product): List[String] = cols match {
      case col: Column[_] =>
        List(singleColumn(col, affinities))
      case product =>
        product.productIterator.toList.flatMap {component =>
          columnHqls(component.asInstanceOf[Product])
        }
    }

    columnHqls(cols).distinct.sorted.mkString(", ")
  }

  // TODO: Test in isolation.
  private def singleColumn(c: Column[_], affinity: Affinities): Hql = c match {
    case sourceColumn: SourceColumn[_] =>
      s"${affinityToLetter(affinity.find{_._1 eq sourceColumn}.get._2)}.${sourceColumn.name}"

    case Equals(left, right) =>
      s"${singleColumn(left, affinity)} = ${singleColumn(right, affinity)}"

    case GreaterThanOrEquals(left, right) =>
      s"${singleColumn(left, affinity)} >= ${singleColumn(right, affinity)}"

    case And(left, right) =>
      s"${singleColumn(left, affinity)} AND ${singleColumn(right, affinity)}"

    case Multiplication(left, right) =>
      s"${singleColumn(left, affinity)} * ${singleColumn(right, affinity)}"

    case LiteralStr(s) =>
      s""""$s""""

    case LiteralNum(n) =>
      n.toString

    case unimplemented =>
      throw new NotImplementedError(s"Not Implemented: toHql($unimplemented)")
  }

  private def affinityToLetter(affinity: Affinity): String =
    ('A' to 'Z')(affinity).toString
}