package sqlpt

// TODO: Restrict column types.
trait Rows[A <: Product] {
  def cols: A

  def join[B <: Product](right: Rows[B])(on: (A, B) => Comparison) =
    InnerJoin(this, right)(on)

  def groupBy[G <: Product](selectGroupingCols: A => G) =
    Grouped[G, A](selectGroupingCols(cols), this, Set.empty)
}

trait Selectable[A <: Product] {this: Rows[A] =>
  def select[B <: Product](p: A => B): Selection[B, A] =
    Selection(p(cols), this, Set.empty)

  def selectDistinct[B <: Product](p: A => B): Selection[B, A] =
    select(p).distinct
}

case class Selection[A <: Product, S <: Product](
  cols: A,
  source: Rows[S],
  filters: Set[Comparison],
  isDistinct: Boolean = false
) extends Rows[A] {
  def distinct: Selection[A, S] =
    copy(isDistinct = true)

  def where(f: S => Comparison): Selection[A, S] =
    copy(filters = filters + f(source.cols))
}

case class InnerJoin[A <: Product, B <: Product](
  left:  Rows[A],
  right: Rows[B]
)(on: (A, B) => Comparison) extends Rows[(A, B)] with Selectable[(A, B)] {
  override def cols = (left.cols, right.cols)
}

case class Table[A <: Product](name: String, cols: A) extends Rows[A] with Selectable[A]

trait TableDef {
  type Columns <: Product
  def name: String
  def cols: Columns

  final def table = Table(name, cols)

  protected implicit def str2StrColumn(colName: String): Str = Str(name, colName)
  protected implicit def str2NumColumn(colName: String): Num = Num(name, colName)
}

sealed trait Column {
  def table: String
  def name:  String

  def ===[B](right: B) =
    Equality(this, right)
}
case class Str(table: String, name: String) extends Column
case class Num(table: String, name: String) extends Column

sealed trait Comparison
case class Equality[A, B](left: A, right: B) extends Comparison

sealed trait AggregationOp
case class Count(col: Column) extends AggregationOp
case class Sum(col: Column) extends AggregationOp

case class Grouped[G <: Product, S <: Product](groupingCols: G, source: Rows[S], sourceFilters: Set[Comparison]) {
  class Aggregator {
    def count(c: S => Column) =
      Count(c(source.cols))
  }

  def select[A <: Product](f: (G, Aggregator) => A) =
    Aggregation(f(groupingCols, new Aggregator), groupingCols, source, sourceFilters, Set.empty)
}

case class Aggregation[A <: Product, G <: Product, S <: Product](
  cols: A,
  groupingCols: G,
  source: Rows[S],
  sourceFilters: Set[Comparison],
  groupFilters: Set[Comparison]
) extends Rows[A] {
  def having(f: A => Comparison) =
    copy(groupFilters = groupFilters + f(cols))
}

object Usage {
  implicit def table2Selection[A <: Product](t: Table[A]): Selection[A, A] =
    Selection(t.cols, t, Set.empty[Comparison])

  object CarLoans extends TableDef {
    val name = "car_loans"

    case class Columns(
      customerId: Str = "cust_id",
      amount:     Num = "amnt"
    )

    val cols = Columns()
  }

  object CreditCards extends TableDef {
    val name = "credit_cards"

    case class Columns(
      cardId:     Str = "card_id",
      customerId: Str = "cust_id",
      expiryDate: Str = "expy_d"
    )

    val cols = Columns()
  }

  val selection = CarLoans.table.select(_.customerId).where(_.amount === 22)

  val filtered = CarLoans.table.where {_.amount === 100}

  val joined = selection.join(CreditCards.table) {case (custId, creditCard) => custId === creditCard.customerId}

  joined.select {case (custId, _) => custId}.where {case (_, cc) => cc.cardId === "zcv"}.distinct

  CreditCards.table
    .groupBy {r => (r.customerId, r.expiryDate)}
    .select {case ((custId, expD), agg) => (
      expD,
      agg.count(_.cardId)
    )}//.having {case (_, count) =>
//      count === 3   // TODO: Doesn't compile.
//    }
}