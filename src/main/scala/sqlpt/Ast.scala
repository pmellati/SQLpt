package sqlpt

import sqlpt.Column._, Type._, Arithmetic._, AggregationFuncs._

// TODO: Restrict column types.
trait Rows[A <: Product] {
  def cols: A

  def join[B <: Product](right: Rows[B])(on: (A, B) => Column[Bool]) =
    InnerJoin(this, right)(on)
}

case class Filtered[S <: Product](source: Rows[S], sourceFilters: Set[Column[Bool]]) {
  def where(f: S => Column[Bool]): Filtered[S] =
    copy(sourceFilters = sourceFilters + f(source.cols))

  def select[B <: Product](p: S => B): Selection[B, S] =
    Selection(p(source.cols), source, sourceFilters)

  def selectDistinct[B <: Product](p: S => B): Selection[B, S] =
    select(p).distinct

  def groupBy[G <: Product](selectGroupingCols: S => G) =
    Grouped[G, S](selectGroupingCols(source.cols), source, sourceFilters)
}

case class Selection[A <: Product, S <: Product](
  cols: A,
  source: Rows[S],
  filters: Set[Column[Bool]],
  isDistinct: Boolean = false
) extends Rows[A] {
  def distinct: Selection[A, S] =
    copy(isDistinct = true)
}

case class InnerJoin[A <: Product, B <: Product](
  left:  Rows[A],
  right: Rows[B]
)(on: (A, B) => Column[Bool]) extends Rows[(A, B)] {
  override def cols = (left.cols, right.cols)
}

case class Table[A <: Product](name: String, cols: A) extends Rows[A]

trait TableDef {
  type Columns <: Product
  def name: String
  def cols: Columns

  final def table = Table(name, cols)

  protected implicit def str2StrColumn(colName: String): Column[Str] = SourceColumn[Str](name, colName)
  protected implicit def str2NumColumn(colName: String): Column[Num] = SourceColumn[Num](name, colName)
}

case class Grouped[G <: Product, S <: Product](groupingCols: G, source: Rows[S], sourceFilters: Set[Column[Bool]]) {
  class Aggregator {
    def count(c: S => Column[_ <: Type]) =
      Count(c(source.cols))
  }

  def select[A <: Product](f: (G, Aggregator) => A) =
    Aggregation(f(groupingCols, new Aggregator), groupingCols, source, sourceFilters, Set.empty)
}

case class Aggregation[A <: Product, G <: Product, S <: Product](
  cols: A,
  groupingCols: G,
  source: Rows[S],
  sourceFilters: Set[Column[Bool]],
  groupFilters: Set[Column[Bool]]
) extends Rows[A] {
  def having(f: A => Column[Bool]) =
    copy(groupFilters = groupFilters + f(cols))
}

object Usage {
  implicit def rows2Filtered[A <: Product](rows: Rows[A]): Filtered[A] =
    Filtered(rows, Set.empty)

  object CarLoans extends TableDef {
    val name = "car_loans"

    case class Columns(
      customerId: Column[Str] = "cust_id",
      amount:     Column[Num] = "amnt"
    )

    val cols = Columns()
  }

  object CreditCards extends TableDef {
    val name = "credit_cards"

    case class Columns(
      cardId:     Column[Str] = "card_id",
      customerId: Column[Str] = "cust_id",
      expiryDate: Column[Str] = "expy_d"
    )

    val cols = Columns()
  }

  import Literal._

  val selection = CarLoans.table.where(_.amount === 22).select(_.customerId)

  val joined = selection.join(CreditCards.table) {case (custId, creditCard) => custId === creditCard.customerId}

  joined
    .where {case (_, cc) => cc.cardId === "zcv"}
    .selectDistinct {case (custId, _) => custId}

  CreditCards.table
    .where {_.expiryDate === "2016-12-21"}
    .where {_.customerId === "CI232354362"}
    .groupBy {r => (r.customerId, r.expiryDate)}
    .select {case ((custId, expD), agg) => (
      expD,
      agg.count(_.cardId)
    )}.having {case (_, count) =>
      count === 3
    }
}