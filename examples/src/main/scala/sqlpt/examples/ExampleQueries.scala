package sqlpt.examples

import sqlpt.api._

object ExampleQueries extends sqlpt.column.ColumnImplicits {
  object CarLoans extends TableDef with NoPartitioning {
    val name = "car_loans"

    case class Columns(
      customerId: Column[Str],
      amount:     Column[Num]
    )
  }

  object CreditCards extends TableDef with NoPartitioning {
    val name = "credit_cards"

    case class Columns(
      cardId:     Column[Str],
      customerId: Column[Str],
      expiryDate: Column[Nullable[Str]]
    )
  }

  val selection = CarLoans.table.where(_.amount === 22).select(_.customerId)

  val joined = selection.join(CreditCards.table) {case (custId, creditCard) => custId === creditCard.customerId}

  joined
    .where {case (_, cc) => cc.cardId === "zcv"}
    .selectDistinct {case (custId, _) => custId}

  CreditCards.table
    .where {_.expiryDate.map(_ === "2016-12-21")}
    .where {_.customerId === "CI232354362"}
    .groupBy {r => (r.customerId, r.expiryDate)}
    .select {case ((custId, expD), agg) => (
      expD,
      agg.count(_.cardId)
    )}.having {case (_, count) =>
      count === 3
    }

  CarLoans.table
    .leftJoin (CreditCards.table) {_.customerId === _.customerId}
    .rightJoin(CarLoans.table) {case (carLoan1, _, carLoan2) => carLoan1.customerId === carLoan2.customerId}
    .select {case (_, cc, _) => cc(_.cardId).isNull}
}
