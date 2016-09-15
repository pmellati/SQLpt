package sqlpt.columndesign1


sealed trait Column[+T <: Column.Type]

object Column {
  sealed trait Type

  object Type {
    case object Str extends Type
    case object Num extends Type    // We support the same ops for all numeric types, so they are all represented by `Num`.

    case class Nullable[T <: Type]() extends Type

    type Str = Str.type
    type Num = Num.type
  }

  import Type._

  case class SourceColumn[T <: Type](tableName: String, name: String) extends Column[T]

  object Arithmetic {
    case class Multiplication(left: Column[Num], right: Column[Num]) extends Column[Num]

    implicit class NumOps(self: Column[Num]) {
      def *(other: Column[Num]) =
        Multiplication(self, other)
    }
  }

  object Aggregation {
    case class Count(col: Column[Type]) extends Column[Num]
    case class Sum(col: Column[Num]) extends Column[Num]
    case class Max[T <: Type](col: Column[T]) extends Column[T]
  }

  object Literal {
    case class LiteralNum(n: Double) extends Column[Type.Num]

    implicit def toLiteralNum(n: Double): LiteralNum = LiteralNum(n)
  }

  object Usage {
    implicit class NullableOps[C[_ <: Type] <: Column[_], T <: Type](col: C[Nullable[T]]) {
      def isNull: Column[Num] = ???
    }

    val balance = SourceColumn[Num]("credit_cards", "balance")
    val custId  = SourceColumn[Str]("credit_cards", "cust_id")

    import Arithmetic._
    import Aggregation._
    import Literal._

    val tripledCol = balance * 3.0      // Should compile.
    val powerOfTwo = balance * balance

    val maxNum = Max(balance * -1.0)
    val maxCustId = Max(custId)
//    val tripledStr = custId * 3       // Shouldn't compile.

    val custAge = SourceColumn[Nullable[Num]]("credit_cards", "cust_age")

    custAge.isNull

//    custAge * 5.0
  }
}
