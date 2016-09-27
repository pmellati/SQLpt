package sqlpt

import sqlpt.Column._, Type._, Arithmetic._, AggregationFuncs._

// TODO: Restrict column types.
trait Rows[Cols <: Product] {
  private[sqlpt] def cols(n: Nothing): Cols

  def join[OtherCols <: Product](right: Rows[OtherCols])(on: (Cols, OtherCols) => Column[Bool]) =
    InnerJoin(this, right, on)
}

case class Filtered[Src <: Product](source: Rows[Src], sourceFilters: Set[Src => Column[Bool]]) {
  def where(f: Src => Column[Bool]): Filtered[Src] =
    copy(sourceFilters = sourceFilters + f)

  def select[Cols <: Product](p: Src => Cols): Selection[Cols, Src] =
    Selection(p, source, sourceFilters)

  def selectDistinct[Cols <: Product](p: Src => Cols): Selection[Cols, Src] =
    select(p).distinct

  def groupBy[GrpCols <: Product](selectGroupingCols: Src => GrpCols) =
    Grouped[GrpCols, Src](selectGroupingCols, source, sourceFilters)
}

case class Selection[Cols <: Product, Src <: Product](
  projection: Src => Cols,
  source:     Rows[Src],
  filters:    Set[Src => Column[Bool]],   // TODO: Does this need to be a Set? We can AND.
  isDistinct: Boolean = false
) extends Rows[Cols] {
  private[sqlpt] override def cols(n: Nothing) =
    projection(source.cols(n))

  def distinct: Selection[Cols, Src] =
    copy(isDistinct = true)
}

case class InnerJoin[LeftCols <: Product, RightCols <: Product](
  left:  Rows[LeftCols],
  right: Rows[RightCols],
  on:    (LeftCols, RightCols) => Column[Bool]
) extends Rows[(LeftCols, RightCols)] {
  private[sqlpt] override def cols(n: Nothing) =
    (left.cols(n), right.cols(n))
}

case class Table[Cols <: Product](name: String, columns: Cols) extends Rows[Cols] {
  private[sqlpt] override def cols(n: Nothing) =
    columns
}

trait TableDef {
  type Columns <: Product
  def name: String
  def cols: Columns

  final def table = Table(name, cols)
  
  protected implicit def str2Column[T <: Type](colName: String): Column[T] = SourceColumn[T](name, colName)
}

case class Grouped[GrpCols <: Product, Src <: Product](
  groupingCols:  Src => GrpCols,
  source:        Rows[Src],
  sourceFilters: Set[Src => Column[Bool]]
) {
  class Aggregator {
    def count(c: Src => Column[_ <: Type]) =
      Count(c)
  }

  def select[Cols <: Product](f: (GrpCols, Aggregator) => Cols) = {
    val columnSelection: (Src, Src => GrpCols) => Cols = {(s, s2g) =>
      f(s2g(s), new Aggregator)
    }

    Aggregation[Cols, GrpCols, Src](columnSelection, groupingCols, source, sourceFilters, Set.empty)
  }
}

case class Aggregation[Cols <: Product, GrpCols <: Product, Src <: Product](
  projection:    (Src, Src => GrpCols) => Cols,
  groupingCols:  Src => GrpCols,
  source:        Rows[Src],
  sourceFilters: Set[Src => Column[Bool]],
  groupFilters:  Set[Cols => Column[Bool]]
) extends Rows[Cols] {
  private[sqlpt] override def cols(n: Nothing): Cols =
    projection(source.cols(n), groupingCols)

  def having(f: Cols => Column[Bool]) =
    copy(groupFilters = groupFilters + f)
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