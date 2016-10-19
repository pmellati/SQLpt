package sqlpt

import sqlpt.Column.NullableOps.IsNull
import scalaz.NonEmptyList

sealed trait Column[+T <: Column.Type] extends Product

object Column {
  sealed trait Type

  object Type {
    case object Str  extends Type
    case object Num  extends Type    // We support the same ops for all numeric types, so they are all represented by `Num`.
    case object Bool extends Type

    case class Nullable[T <: Type]() extends Type

    type Str  = Str.type
    type Num  = Num.type
    type Bool = Bool.type
  }

  import Type._

  case class SourceColumn[T <: Type](tableName: String, name: String) extends Column[T]

  object Arithmetic {
    case class Multiplication(left: Column[Num], right: Column[Num]) extends Column[Num]
    case class Equals[T <: Type](left: Column[T], right: Column[T]) extends Column[Bool]
    case class NotEquals[T <: Type](left: Column[T], right: Column[T]) extends Column[Bool]
    case class GreaterThanOrEquals[T <: Type](left: Column[T], right: Column[T]) extends Column[Bool]
    case class And(left: Column[Bool], right: Column[Bool]) extends Column[Bool]
    case class Or(left: Column[Bool], right: Column[Bool]) extends Column[Bool]

    implicit class NumOps(self: Column[Num]) {
      def *(other: Column[Num]) =
        Multiplication(self, other)
    }

    implicit class ComparisonOps[T <: Type](self: Column[T]) {
      def ===(other: Column[T]) =
        Equals(self, other)

      def =!=(other: Column[T]) =
        Equals(self, other)

      def >=(other: Column[T]) =
        GreaterThanOrEquals(self, other)
    }

    implicit class BoolColumnOps (self: Column[Bool]) {
      def and(other: Column[Bool]) =
        And(self, other)

      def or(other: Column[Bool]) =
        Or(self, other)
    }
  }

  object AggregationFuncs {
    case class Count         (col: Column[_ <: Type]) extends Column[Num]
    case class Sum           (col: Column[Num])       extends Column[Num]
    case class Max[T <: Type](col: Column[T])         extends Column[T]
  }

  object WindowingAndAnalytics {
    import AnalyticsFunc._

    object AnalyticsFunc {
      case class RowNumber(over: Over) extends Column[Num]
      case class Rank     (over: Over) extends Column[Num]
    }

    case class Over(partitionByCols: NonEmptyList[Column[_ <: Type]], orderByCols: List[Column[_ <: Type]] = Nil) {
      def orderBy(cols: Column[_ <: Type]*) =
        copy(orderByCols = cols.toList)

      def rowNumber = RowNumber(this)
      def rank      = Rank(this)
    }

    def partitionBy(col1: Column[_ <: Type], cols: Column[_ <: Type]*) =
      Over(NonEmptyList(col1, cols: _*))
  }

  object Literal {
    case class LiteralNum (n: Double)  extends Column[Num]
    case class LiteralStr (s: String)  extends Column[Str]
    case class LiteralBool(b: Boolean) extends Column[Bool]

    implicit def literal[N : Numeric](n: N): LiteralNum =
      LiteralNum(implicitly[Numeric[N]].toDouble(n))

    implicit def literal(s: String): LiteralStr =
      LiteralStr(s)

    implicit def literal(b: Boolean): LiteralBool =
      LiteralBool(b)
  }

  object MiscFunctions {
    // TODO: Restrict type `T`?
    case class IsBetween[T <: Type](col: Column[T], lowerBound: Column[T], upperBound: Column[T]) extends Column[Bool]
    case class IsIn[T <: Type](col: Column[T], values: Seq[Column[T]]) extends Column[Bool]
    case class IsNotIn[T <: Type](col: Column[T], values: Seq[Column[T]]) extends Column[Bool]

    case class Aliased[T <: Type](col: Column[T], alias: String) extends Column[T]

    implicit class Syntax[T <: Type](col: Column[T]) {
      def isBetween(lowerBound: Column[T], upperBound: Column[T]) =
        IsBetween(col, lowerBound, upperBound)

      def isIn(values: Seq[Column[T]]) =
        IsIn(col, values)

      def isIn(value1: Column[T], values: Column[T]*) =
        IsIn(col, value1 +: values)

      // TODO: Can we get away with just defining a general 'negation' operator and then reusing `isIn`?
      def isNotIn(values: Seq[Column[T]]) =
        IsNotIn(col, values)

      def isNotIn(value1: Column[T], values: Column[T]*) =
        IsNotIn(col, value1 +: values)

      def toNullable: Column[Nullable[T]] =
        col.asInstanceOf[Column[Nullable[T]]]

      def as(alias: String) =
        Aliased(col, alias)
    }
  }

  object NullableOps {
    // TODO: Can type param 'C' be removed?
    case class IsNull[C[_ <: Type] <: Column[_], T <: Type](column: C[Nullable[T]]) extends Column[Bool]
  }

  implicit class RichColumnOfNullable[C[_ <: Type] <: Column[_], T <: Type](col: C[Nullable[T]]) {
    def isNull: Column[Bool] =
      IsNull(col)

    def map[C2[_ <: Type] <: Column[_], T2 <: Type](f: C[T] => C2[T2]): C2[Nullable[T2]] =
      f(get).asInstanceOf[C2[Nullable[T2]]]

    def flatMap[C2[_ <: Type] <: Column[_], T2 <: Type](f: C[T] => C2[Nullable[T2]]): C2[Nullable[T2]] =
      f(get)

    def get: C[T] =
      col.asInstanceOf[C[T]]
  }

  object Date {
    case class UnixTimestamp(date: Column[Str], pattern: String) extends Column[Num]
    case class FromUnixTime(timestamp: Column[Num]) extends Column[Str]
    case class ToDate(timeStr: Column[Str]) extends Column[Str]
    case class DateAdd(timeStr: Column[Str], days: Column[Num]) extends Column[Str]

    // TODO: Add syntax / enrich types.
  }

  object Usage {
    val balance = SourceColumn[Num]("credit_cards", "balance")
    val custId  = SourceColumn[Str]("credit_cards", "cust_id")

    import Arithmetic._
    import AggregationFuncs._
    import Literal._

    val tripledCol = balance * 3.0      // Should compile.
    val powerOfTwo = balance * balance

    val maxNum = Max(balance * -1.0)
    val maxCustId = Max(custId)
//    val tripledStr = custId * 3       // Shouldn't compile.

    val custAge = SourceColumn[Nullable[Num]]("credit_cards", "cust_age")

    custAge.isNull

    val multipliedNullable = custAge.map(_ * 5.0)

    val twoNullables = custAge.flatMap(age1 => custAge.map(age2 => age1 * age2))
  }
}
