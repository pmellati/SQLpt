package sqlpt.examples

import sqlpt.column._, Column._, Type._

object ColumnAlgebra extends ColumnImplicits {
  import AggregationFuncs.Max

  val balance = SourceColumn[Num]("credit_cards", "balance")
  val custId  = SourceColumn[Str]("credit_cards", "cust_id")

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
