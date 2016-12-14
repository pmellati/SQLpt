package sqlpt.ast.expressions

import sqlpt._, column._, Column._, AggregationFuncs._, Type._

sealed trait Rows[Cols <: Product] {
  def cols: Cols

  def join[OtherCols <: Product](right: Rows[OtherCols])(on: (Cols, OtherCols) => Column[Bool]) =
    Joined2[Cols, OtherCols, Cols, OtherCols](this, right, Seq(on(this.cols, right.cols)), Seq(JoinMode.Inner))

  // TODO: Remove direct call to `Outer`.
  def leftJoin[OtherCols <: Product](right: Rows[OtherCols])(on: (Cols, OtherCols) => Column[Bool]) =
    Joined2[Cols, Nullabled[OtherCols], Cols, OtherCols](this, Outer(right), Seq(on(this.cols, right.cols)), Seq(JoinMode.Left))

  // TODO: Remove direct call to `Outer`.
  def rightJoin[OtherCols <: Product](right: Rows[OtherCols])(on: (Cols, OtherCols) => Column[Bool]) =
    Joined2[Nullabled[Cols], OtherCols, Cols, OtherCols](Outer(this), right, Seq(on(this.cols, right.cols)), Seq(JoinMode.Right))
}

case class Filtered[Src <: Product](source: Rows[Src], sourceFilters: Set[Column[Bool]]) {
  def where(f: Src => Column[Bool]): Filtered[Src] =
    copy(sourceFilters = sourceFilters + f(source.cols))

  def select[Cols <: Product : Columns](p: Src => Cols): SimpleSelection[Cols, Src] =
    SimpleSelection(p(source.cols), source, sourceFilters)

  def selectDistinct[Cols <: Product : Columns](p: Src => Cols): SimpleSelection[Cols, Src] =
    select(p).distinct

  def groupBy[GrpCols <: Product : Columns](selectGroupingCols: Src => GrpCols) =
    Grouped[GrpCols, Src](selectGroupingCols(source.cols), source, sourceFilters)
}

// TODO: Can the type parameter be made covariant?
sealed trait Selection[Cols <: Product] extends Rows[Cols] {
  def cols: Cols

  def unionAll(other: Selection[Cols]) =
    UnionAll(Seq(this, other))
}

case class SimpleSelection[Cols <: Product, Src <: Product](
  cols:       Cols,
  source:     Rows[Src],
  filters:    Set[Column[Bool]],   // TODO: Does this need to be a Set? We can AND.
  isDistinct: Boolean = false
) extends Selection[Cols] {
  def distinct: SimpleSelection[Cols, Src] =
    copy(isDistinct = true)
}

case class Nullabled[Cols <: Product](cols: Cols) {
  def apply[T <: Type](f: Cols => Column[T]) =
    f(cols).asInstanceOf[Column[Nullable[T]]]
}

case class Outer[Cols <: Product] private (rows: Rows[Cols]) extends Rows[Nullabled[Cols]] {
  override def cols = Nullabled(rows.cols)
}

sealed trait JoinMode
object JoinMode {
  case object Inner extends JoinMode
  case object Left  extends JoinMode
  case object Right extends JoinMode
}

trait BaseJoined {
  def ons: Seq[Column[Bool]]    // TODO: Define type alias 'JoinCondition' for 'Column[Bool]'.
  def joinModes: Seq[JoinMode]
  def sourceSeq: Seq[Rows[_ <: Product]]

  protected def outer[Inner <: Product](rows: Rows[_ <: Product]): Outer[Inner] = rows match {
    case alreadyOuter: Outer[_] => alreadyOuter.asInstanceOf[Outer[Inner]]
    case nonOuter => Outer(nonOuter).asInstanceOf[Outer[Inner]]
  }
}

case class Joined2
  [Cols1 <: Product, Cols2 <: Product, Inner1 <: Product, Inner2 <: Product]
  (rows1: Rows[Cols1], rows2: Rows[Cols2], ons: Seq[Column[Bool]], joinModes: Seq[JoinMode])
  extends Rows[(Cols1, Cols2)]
  with BaseJoined
{
  override def cols =
    (rows1.cols, rows2.cols)

  override def sourceSeq =
    Seq(rows1, rows2)

  def join[NewCols <: Product](newRows: Rows[NewCols])(on: (Cols1, Cols2, NewCols) => Column[Bool]) =
    Joined3[Cols1, Cols2, NewCols, Inner1, Inner2, NewCols](
      rows1, rows2, newRows, ons :+ on(rows1.cols, rows2.cols, newRows.cols), joinModes :+ JoinMode.Inner)

  def leftJoin[NewCols <: Product](newRows: Rows[NewCols])(on: (Cols1, Cols2, NewCols) => Column[Bool]) =
    Joined3[Cols1, Cols2, Nullabled[NewCols], Inner1, Inner2, NewCols](
      rows1, rows2, outer(newRows), ons :+ on(rows1.cols, rows2.cols, newRows.cols), joinModes :+ JoinMode.Left)

  def rightJoin[NewCols <: Product](newRows: Rows[NewCols])(on: (Cols1, Cols2, NewCols) => Column[Bool]) =
    Joined3[Nullabled[Inner1], Nullabled[Inner2], NewCols, Inner1, Inner2, NewCols](
      outer(rows1), outer(rows2), newRows, ons :+ on(rows1.cols, rows2.cols, newRows.cols), joinModes :+ JoinMode.Right)
}

case class Joined3
  [Cols1 <: Product, Cols2 <: Product, Cols3 <: Product, Inner1 <: Product, Inner2 <: Product, Inner3 <: Product]
  (rows1: Rows[Cols1], rows2: Rows[Cols2], rows3: Rows[Cols3], ons: Seq[Column[Bool]], joinModes: Seq[JoinMode])
  extends Rows[(Cols1, Cols2, Cols3)]
  with BaseJoined
{
  override def cols =
    (rows1.cols, rows2.cols, rows3.cols)

  override def sourceSeq =
    Seq(rows1, rows2, rows3)

  def join[NewCols <: Product](newRows: Rows[NewCols])(on: (Cols1, Cols2, Cols3, NewCols) => Column[Bool]) =
    Joined4[Cols1, Cols2, Cols3, NewCols, Inner1, Inner2, Inner3, NewCols](
      rows1, rows2, rows3, newRows, ons :+ on(rows1.cols, rows2.cols, rows3.cols, newRows.cols), joinModes :+ JoinMode.Inner)

  def leftJoin[NewCols <: Product](newRows: Rows[NewCols])(on: (Cols1, Cols2, Cols3, NewCols) => Column[Bool]) =
    Joined4[Cols1, Cols2, Cols3, Nullabled[NewCols], Inner1, Inner2, Inner3, NewCols](
      rows1, rows2, rows3, outer(newRows), ons :+ on(rows1.cols, rows2.cols, rows3.cols, newRows.cols), joinModes :+ JoinMode.Left)

  def rightJoin[NewCols <: Product](newRows: Rows[NewCols])(on: (Cols1, Cols2, Cols3, NewCols) => Column[Bool]) =
    Joined4[Nullabled[Inner1], Nullabled[Inner2], Nullabled[Inner3], NewCols, Inner1, Inner2, Inner3, NewCols](
      outer(rows1), outer(rows2), outer(rows3), newRows, ons :+ on(rows1.cols, rows2.cols, rows3.cols, newRows.cols), joinModes :+ JoinMode.Right)
}

case class Joined4
  [Cols1 <: Product, Cols2 <: Product, Cols3 <: Product, Cols4 <: Product, Inner1 <: Product, Inner2 <: Product, Inner3 <: Product, Inner4 <: Product]
  (rows1: Rows[Cols1], rows2: Rows[Cols2], rows3: Rows[Cols3], rows4: Rows[Cols4], ons: Seq[Column[Bool]], joinModes: Seq[JoinMode])
  extends Rows[(Cols1, Cols2, Cols3, Cols4)]
  with BaseJoined
{
  override def cols =
    (rows1.cols, rows2.cols, rows3.cols, rows4.cols)

  override def sourceSeq =
    Seq(rows1, rows2, rows3, rows4)

  def join[NewCols <: Product](newRows: Rows[NewCols])(on: (Cols1, Cols2, Cols3, Cols4, NewCols) => Column[Bool]) =
    Joined5[Cols1, Cols2, Cols3, Cols4, NewCols, Inner1, Inner2, Inner3, Inner4, NewCols](
      rows1, rows2, rows3, rows4, newRows, ons :+ on(rows1.cols, rows2.cols, rows3.cols, rows4.cols, newRows.cols), joinModes :+ JoinMode.Inner)

  def leftJoin[NewCols <: Product](newRows: Rows[NewCols])(on: (Cols1, Cols2, Cols3, Cols4, NewCols) => Column[Bool]) =
    Joined5[Cols1, Cols2, Cols3, Cols4, Nullabled[NewCols], Inner1, Inner2, Inner3, Inner4, NewCols](
      rows1, rows2, rows3, rows4, outer(newRows), ons :+ on(rows1.cols, rows2.cols, rows3.cols, rows4.cols, newRows.cols), joinModes :+ JoinMode.Left)

  def rightJoin[NewCols <: Product](newRows: Rows[NewCols])(on: (Cols1, Cols2, Cols3, Cols4, NewCols) => Column[Bool]) =
    Joined5[Nullabled[Inner1], Nullabled[Inner2], Nullabled[Inner3], Nullabled[Inner4], NewCols, Inner1, Inner2, Inner3, Inner4, NewCols](
      outer(rows1), outer(rows2), outer(rows3), outer(rows4), newRows, ons :+ on(rows1.cols, rows2.cols, rows3.cols, rows4.cols, newRows.cols), joinModes :+ JoinMode.Right)
}

case class Joined5
  [Cols1 <: Product, Cols2 <: Product, Cols3 <: Product, Cols4 <: Product, Cols5 <: Product, Inner1 <: Product, Inner2 <: Product, Inner3 <: Product, Inner4 <: Product, Inner5 <: Product]
  (rows1: Rows[Cols1], rows2: Rows[Cols2], rows3: Rows[Cols3], rows4: Rows[Cols4], rows5: Rows[Cols5], ons: Seq[Column[Bool]], joinModes: Seq[JoinMode])
  extends Rows[(Cols1, Cols2, Cols3, Cols4, Cols5)]
  with BaseJoined
{
  override def cols =
    (rows1.cols, rows2.cols, rows3.cols, rows4.cols, rows5.cols)

  override def sourceSeq =
    Seq(rows1, rows2, rows3, rows4, rows5)
}

case class Table[Cols <: Product](name: String, cols: Cols) extends Rows[Cols]

case class Grouped[GrpCols <: Product, Src <: Product](
  groupingCols:  GrpCols,
  source:        Rows[Src],
  sourceFilters: Set[Column[Bool]]
) {
  class Aggregator {
    def count(s: Src => Column[_ <: Type]) =
      Count(s(source.cols))

    def sum(s: Src => Column[Num]) =
      Sum(s(source.cols))

    def max[T <: Type](s: Src => Column[T]) =
      Max(s(source.cols))
  }

  def select[Cols <: Product : Columns](f: (GrpCols, Aggregator) => Cols) =
    AggrSelection[Cols, GrpCols, Src](f(groupingCols, new Aggregator), groupingCols, source, sourceFilters, Set.empty)
}

// TODO: Do these support 'DISTINCT' in SQL?
case class AggrSelection[Cols <: Product, GrpCols <: Product, Src <: Product](
  cols:          Cols,
  groupingCols:  GrpCols,
  source:        Rows[Src],
  sourceFilters: Set[Column[Bool]],
  groupFilters:  Set[Column[Bool]]
) extends Selection[Cols] {
  def having(f: Cols => Column[Bool]) =
    copy(groupFilters = groupFilters + f(cols))
}

case class UnionAll[Cols <: Product] private (selects: Seq[Selection[Cols]]) extends Rows[Cols] {
  override def cols = selects.head.cols   // TODO: Does this make sense?
}
