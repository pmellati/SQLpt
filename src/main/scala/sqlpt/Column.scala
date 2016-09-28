package sqlpt

import sqlpt.Column.NullableOps.IsNull


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
    case class And(left: Column[Bool], right: Column[Bool]) extends Column[Bool]

    implicit class NumOps(self: Column[Num]) {
      def *(other: Column[Num]) =
        Multiplication(self, other)
    }

    implicit class EqualityOps[T <: Type](self: Column[T]) {
      def ===(other: Column[T]) =
        Equals(self, other)
    }

    implicit class BoolColumnOps (self: Column[Bool]) {
      def and(other: Column[Bool]) =
        And(self, other)
    }
  }

  object AggregationFuncs {
    case class Count(col: Column[_ <: Type]) extends Column[Num]
    case class Sum(col: Column[Num]) extends Column[Num]
    case class Max[T <: Type](col: Column[T]) extends Column[T]
  }

  object Literal {
    case class LiteralNum(n: Double) extends Column[Num]
    case class LiteralStr(s: String) extends Column[Str]

    implicit def toLiteralNum[N : Numeric](n: N): LiteralNum =
      LiteralNum(implicitly[Numeric[N]].toDouble(n))

    implicit def toLiteralStr(s: String): LiteralStr =
      LiteralStr(s)
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