package sqlpt.hive

import sqlpt._, column._, Column._, Arithmetic._, Literals._, AggregationFuncs._, ast._, expressions._
import JoinTranslationHelpers._
import scalaz._, Scalaz._

object Translators extends ColumnImplicits {
  def selection: Translator[Selection[_ <: Product]] = {
    case ss: SimpleSelection[_, _] =>
      simpleSelection(ss)
    case as: AggrSelection[_, _, _] =>
      aggrSelection(as)
  }

  def simpleSelection: Translator[SimpleSelection[_ <: Product, _ <: Product]] = {selection =>
    val affinities = affinitiesOf(selection.source)

    val selectClause = {
      val selectedColumns = columnsProduct(selection.cols, affinities)

      val optionallyDistinct = selection.isDistinct ? "DISTINCT" | ""

      s"SELECT $optionallyDistinct $selectedColumns"
    }

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

    s"""
       |$selectClause
       |$fromClause
       |$whereClause
     """.stripMargin.trim
  }

  def aggrSelection: Translator[AggrSelection[_ <: Product, _ <: Product, _ <: Product]] = {selection =>
    val simpleSelectionTranslation: Hql = {
      val asSimpleSelection = SimpleSelection(selection.cols, selection.source, selection.sourceFilters)
      simpleSelection(asSimpleSelection)
    }

    val affinities = affinitiesOf(selection.source)

    val groupByClause = s"GROUP BY ${columnsProduct(selection.groupingCols, affinities)}"

    val havingClause = {
      val allFilters = selection.groupFilters.reduceOption(_ and _)
      allFilters.fold("") {allFilters =>
        s"HAVING ${singleColumn(allFilters, affinities)}"
      }
    }

    s"""
       |$simpleSelectionTranslation
       |$groupByClause
       |$havingClause
     """.stripMargin.trim
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
  private def singleColumn(c: Column[_], affinities: Affinities): Hql = c match {
    case sourceColumn: SourceColumn[_] =>
      s"${affinityToLetter(affinities.find{_._1 eq sourceColumn}.get._2)}.${sourceColumn.name}"

    case Equals(left, right) =>
      s"${singleColumn(left, affinities)} = ${singleColumn(right, affinities)}"

    case GreaterThanOrEquals(left, right) =>
      s"${singleColumn(left, affinities)} >= ${singleColumn(right, affinities)}"

    case And(left, right) =>
      s"${singleColumn(left, affinities)} AND ${singleColumn(right, affinities)}"

    case Multiplication(left, right) =>
      s"${singleColumn(left, affinities)} * ${singleColumn(right, affinities)}"

    case LiteralStr(s) =>
      s""""$s""""

    case LiteralNum(n) =>
      n.toString

    case Max(col) =>
      s"MAX(${singleColumn(col, affinities)})"

    case Count(col) =>
      s"COUNT(${singleColumn(col, affinities)})"

    case unimplemented =>
      throw new NotImplementedError(s"Not Implemented: toHql($unimplemented)")
  }

  private def affinityToLetter(affinity: Affinity): String =
    ('A' to 'Z')(affinity).toString
}