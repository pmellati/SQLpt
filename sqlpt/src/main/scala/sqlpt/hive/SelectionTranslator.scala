package sqlpt.hive

import sqlpt._, column._, Column._, Type._, Arithmetic._, Literals._, ast._, expressions._
import JoinTranslationHelpers._
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

    val selectedColumns = columnsToHql(selection.cols, affinities)

    val optionallyDistinct = selection.isDistinct ? "DISTINCT" | ""

    val fromClause = {
      def src2Str(src: Rows[_ <: Product]): String = {
        val needsParentheses =
          !src.isInstanceOf[Table[_]] && !src.isInstanceOf[Outer[_]]

        val (optParenOpen, optParenClose) = needsParentheses ? ("(", ")") | ("", "")

        s"$optParenOpen ${Internal.toHql(src)} $optParenClose"
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

            val onClause = s"ON ${toHql(joinCond, affinities)}"

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
        s"WHERE ${toHql(allFilters, affinities)}"
      }
    }

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
        SelectionTranslator(selection)
      case table: Table[_] =>
        SelectionTranslator.toHql(table)
      case Outer(r) =>
        toHql(r)
      case unimplemented =>
        throw new NotImplementedError(s"Not Implemented: Internal.toHql($unimplemented)")
    }
  }


  def columnsToHql(cols: Product, affinities: Affinities): Hql = {
    def columnHqls(cols: Product): List[String] = cols match {
      case singleColumn: Column[_] =>
        List(toHql(singleColumn, affinities))
      case product =>
        product.productIterator.toList.flatMap {component =>
          columnHqls(component.asInstanceOf[Product])
        }
    }

    columnHqls(cols).distinct.sorted.mkString(", ")
  }

  // TODO: Test individually.
  def toHql(column: Column[_], affinity: Affinities): Hql = column match {
    case sourceColumn: SourceColumn[_] =>
      s"${affinityToLetter(affinity.find{_._1 eq sourceColumn}.get._2)}.${sourceColumn.name}"

    case Equals(left, right) =>
      s"${toHql(left, affinity)} = ${toHql(right, affinity)}"

    case GreaterThanOrEquals(left, right) =>
      s"${toHql(left, affinity)} >= ${toHql(right, affinity)}"

    case And(left, right) =>
      s"${toHql(left, affinity)} AND ${toHql(right, affinity)}"

    case Multiplication(left, right) =>
      s"${toHql(left, affinity)} * ${toHql(right, affinity)}"

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


// TODO: Reconsider the names and put into separate file.
object JoinTranslationHelpers {
  type Affinity      = Int
  type Affinities    = Seq[(SourceColumn[_ <: Type], Affinity)]    // TODO: Should be a Map that uses reference equality as equality for keys.
  type JoinCondition = Column[Bool]

  def affinitiesOf(rows: Rows[_ <: Product]): Affinities =
    sourcesOfJoin(rows).zipWithIndex.foldLeft(Seq.empty: Affinities) {case (affinities, (src, affinity)) =>
      affinities ++ sourceColumnsWithAffinity(src, affinity)
    }

  def joinInfo(joined: BaseJoined):
  (
    Rows[_ <: Product],
    Seq[(Rows[_ <: Product], JoinCondition, JoinMode)]
  ) =
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
    case _ =>
      ???   // TODO
  }
}