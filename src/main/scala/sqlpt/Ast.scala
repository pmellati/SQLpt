package sqlpt

import sqlpt.Column._, Type._, Arithmetic._, AggregationFuncs._

// TODO: Restrict column types.
sealed trait Rows[Cols <: Product] {
  def cols: Cols

  def join[OtherCols <: Product](right: Rows[OtherCols])(on: (Cols, OtherCols) => Column[Bool]) =
    InnerJoin(this, right, on(this.cols, right.cols))

  def leftJoin[OtherCols <: Product](right: Rows[OtherCols])(on: (Cols, OtherCols) => Column[Bool]) =
    LeftJoin(this, right, on(this.cols, right.cols))
}

case class Filtered[Src <: Product](source: Rows[Src], sourceFilters: Set[Column[Bool]]) {
  def where(f: Src => Column[Bool]): Filtered[Src] =
    copy(sourceFilters = sourceFilters + f(source.cols))

  def select[Cols <: Product](p: Src => Cols): SimpleSelection[Cols, Src] =
    SimpleSelection(p(source.cols), source, sourceFilters)

  def selectDistinct[Cols <: Product](p: Src => Cols): SimpleSelection[Cols, Src] =
    select(p).distinct

  def groupBy[GrpCols <: Product](selectGroupingCols: Src => GrpCols) =
    Grouped[GrpCols, Src](selectGroupingCols(source.cols), source, sourceFilters)
}

sealed trait Selection[Cols <: Product] {
  def cols: Cols

  def unionAll(other: Selection[Cols]) =
    UnionAll(Seq(this, other))
}

case class SimpleSelection[Cols <: Product, Src <: Product](
  cols:       Cols,
  source:     Rows[Src],
  filters:    Set[Column[Bool]],   // TODO: Does this need to be a Set? We can AND.
  isDistinct: Boolean = false
) extends Rows[Cols] with Selection[Cols] {
  def distinct: SimpleSelection[Cols, Src] =
    copy(isDistinct = true)
}

case class InnerJoin[LeftCols <: Product, RightCols <: Product](
  left:  Rows[LeftCols],
  right: Rows[RightCols],
  on:    Column[Bool]
) extends Rows[(LeftCols, RightCols)] {
  override def cols =
    (left.cols, right.cols)
}

case class LeftJoin[LeftCols <: Product, RightCols <: Product](
  left:  Rows[LeftCols],
  right: Rows[RightCols],
  on:    Column[Bool]
) extends Rows[(LeftCols, Nullabled[RightCols])] {
  override def cols =
    (left.cols, new Nullabled(right.cols))
}

// TODO: Reconsider this. And should this be a `Column`?
class Nullabled[Cols <: Product](cols: Cols) {
  import MiscFunctions._

  def apply[T <: Type](select: Cols => Column[T]): Column[Nullable[T]] =
    select(cols).asNullable
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

    def sum(s: Src => Column[Num]) =
      Sum(s(source.cols))
  }

  def select[Cols <: Product](f: (GrpCols, Aggregator) => Cols) =
    AggrSelection[Cols, GrpCols, Src](f(groupingCols, new Aggregator), groupingCols, source, sourceFilters, Set.empty)
}

case class AggrSelection[Cols <: Product, GrpCols <: Product, Src <: Product](
  cols:          Cols,
  groupingCols:  GrpCols,
  source:        Rows[Src],
  sourceFilters: Set[Column[Bool]],
  groupFilters:  Set[Column[Bool]]
) extends Rows[Cols] with Selection[Cols] {
  def having(f: Cols => Column[Bool]) =
    copy(groupFilters = groupFilters + f(cols))
}

case class UnionAll[Cols <: Product] private (selects: Seq[Selection[Cols]]) extends Rows[Cols] {
  override def cols = selects.head.cols   // TODO: Does this make sense?
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