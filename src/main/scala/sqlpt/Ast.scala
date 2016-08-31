package sqlpt

// TODO: More than 22 columns?
// TODO: Restrict column types.
trait Rows[A <: Product] {
  def cols: A

  def join[B <: Product](right: Rows[B])(on: (A, B) => Comparison) =
    InnerJoin(this, right)(on)
}

trait Selectable[A <: Product] {this: Rows[A] =>
  def select[B <: Product](p: A => B): Selection[B, A] =
    Selection(p(cols), this, Set.empty)

  def selectDistinct[B <: Product](p: A => B): Selection[B, A] =
    Selection(p(cols), this, Set.empty).distinct
}

case class Selection[A <: Product, S <: Product](
  cols: A,
  source: Rows[S],
  filters: Set[Comparison],
  isDistinct: Boolean = false
) extends Rows[A] {
  def distinct =
    copy(isDistinct = true)

  def where(f: S => Comparison): Selection[A, S] =
    copy(filters = filters + f(source.cols))
}

// Another alternative is to to make this a Rows[A ++ B].
case class InnerJoin[A <: Product, B <: Product](
  left:  Rows[A],
  right: Rows[B]
)(on: (A, B) => Comparison) extends Rows[(A, B)] with Selectable[(A, B)] {
  override def cols = (left.cols, right.cols)
}

case class Table[A <: Product](name: String, cols: A) extends Rows[A] with Selectable[A]

object DefTable {
  class TableColCreator(tableName: String) {
    def colStr(name: String) = Str(tableName, name)
    def colNum(name: String) = Num(tableName, name)
  }

  // TODO: The function can still return a Product of anything.
  def apply[A <: Product](name: String)(f: TableColCreator => A): Table[A] =
    Table(name, f(new TableColCreator(name)))
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

object Usage {
  implicit def table2Selection[A <: Product](t: Table[A]): Selection[A, A] =
    Selection(t.cols, t, Set.empty[Comparison])

  val carLoans = DefTable("car_loans") {c => (
    c.colStr("customer_id"),
    c.colNum("loan_amount")
  )}

  val creditCards = DefTable("credit_cards") {c => (
    c.colStr("card_id"),
    c.colStr("customer_id")
  )}

  val selection = carLoans.select {case (custId, _) => custId}

  val filtered = carLoans.where {case (_, amnt) => amnt === 33}

  val joined = selection.join(creditCards) {case (custId, cc) => custId === cc._1}

  joined.select {case (custId, _) => custId}.where {case (_, (x, y)) => y === "zcv"}.distinct
}