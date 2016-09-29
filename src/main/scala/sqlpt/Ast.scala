package sqlpt

import sqlpt.Column._, Type._, Arithmetic._, AggregationFuncs._

// TODO: Restrict column types.
sealed trait Rows[Cols <: Product] {
  def cols: Cols

  def join[OtherCols <: Product](right: Rows[OtherCols])(on: (Cols, OtherCols) => Column[Bool]) =
    InnerJoin(this, right, on)
}

case class Filtered[Src <: Product](source: Rows[Src], sourceFilters: Set[Column[Bool]]) {
  def where(f: Src => Column[Bool]): Filtered[Src] =
    copy(sourceFilters = sourceFilters + f(source.cols))

  def select[Cols <: Product](p: Src => Cols): Selection[Cols, Src] =
    Selection(p(source.cols), source, sourceFilters)

  def selectDistinct[Cols <: Product](p: Src => Cols): Selection[Cols, Src] =
    select(p).distinct

  def groupBy[GrpCols <: Product](selectGroupingCols: Src => GrpCols) =
    Grouped[GrpCols, Src](selectGroupingCols(source.cols), source, sourceFilters)
}

case class Selection[Cols <: Product, Src <: Product](
  cols:       Cols,
  source:     Rows[Src],
  filters:    Set[Column[Bool]],   // TODO: Does this need to be a Set? We can AND.
  isDistinct: Boolean = false
) extends Rows[Cols] {
  def distinct: Selection[Cols, Src] =
    copy(isDistinct = true)
}

case class InnerJoin[LeftCols <: Product, RightCols <: Product](
  left:  Rows[LeftCols],
  right: Rows[RightCols],
  on:    (LeftCols, RightCols) => Column[Bool]
) extends Rows[(LeftCols, RightCols)] {
  override def cols =
    (left.cols, right.cols)
}

case class Table[Cols <: Product](name: String, cols: Cols) extends Rows[Cols]

trait TableDef {
  type Columns <: Product
  def name: String
  def cols: Columns

  final def table = Table(name, cols)
  
  protected implicit def str2Column[T <: Type](colName: String): Column[T] = SourceColumn[T](name, colName)
}

case class Grouped[GrpCols <: Product, Src <: Product](
  groupingCols:  GrpCols,
  source:        Rows[Src],
  sourceFilters: Set[Column[Bool]]
) {
  class Aggregator {
    def count(c: Src => Column[_ <: Type]) =
      Count(c(source.cols))
  }

  def select[Cols <: Product](f: (GrpCols, Aggregator) => Cols) =
    Aggregation[Cols, GrpCols, Src](f(groupingCols, new Aggregator), groupingCols, source, sourceFilters, Set.empty)
}

case class Aggregation[Cols <: Product, GrpCols <: Product, Src <: Product](
  cols:          Cols,
  groupingCols:  GrpCols,
  source:        Rows[Src],
  sourceFilters: Set[Column[Bool]],
  groupFilters:  Set[Column[Bool]]
) extends Rows[Cols] {
  def having(f: Cols => Column[Bool]) =
    copy(groupFilters = groupFilters + f(cols))
}

object Usage {
  implicit def rows2Filtered[Src <: Product](rows: Rows[Src]): Filtered[Src] =
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
      expiryDate: Column[Nullable[Str]] = "expy_d"
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
    .where {_.expiryDate.map(_ === "2016-12-21")}
    .where {_.customerId === "CI232354362"}
    .groupBy {r => (r.customerId, r.expiryDate)}
    .select {case ((custId, expD), agg) => (
      expD,
      agg.count(_.cardId)
    )}.having {case (_, count) =>
      count === 3
    }
}