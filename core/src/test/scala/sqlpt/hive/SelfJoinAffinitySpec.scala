package sqlpt.hive

import org.specs2.mutable.Spec
import org.specs2.matcher._
import sqlpt._, column._, Column._, Type._, ast.expressions._, Util._
import HqlWriter.{Hql, toHql}
import ColumnsTypeChecking._

class SelfJoinAffinitySpec extends Spec with MatchersCreation {
  object Cars extends TableDef {
    override val name = "cars"

    case class Columns(
      id:    Column[Str] = "car_id",
      price: Column[Num] = "price"
    )

    override def cols = Columns()
  }

  "Affinities should be properly determined in the translated Hql" in {
    val query =
      Cars.table.join(Cars.table) {_.id === _.id}
        .where {case (l, r) =>
          r.price === 12000
        }.select {case (l, r) =>
          (r.id, l.id, l.price * r.price)
        }

    toHql(query) must beSameHqlAs(
      """
        |SELECT B.car_id, A.car_id, A.price * B.price
        |FROM cars A
        |JOIN cars B
        |ON A.car_id = B.car_id
        |WHERE B.price = 12000.0
      """.stripMargin)
  }

  // TODO: Put this in the lib, so that it doesn't have to be defined everywhere.
  private implicit def rows2Filtered[Src <: Product](rows: Rows[Src]): Filtered[Src] =
    Filtered(rows, Set.empty)

  private def beSameHqlAs(other: Hql): Matcher[Hql] = {self: Hql =>
    def tokenized(text: Hql) =
      text.split("\\s").toSeq.filterNot(_.trim.isEmpty)

    (tokenized(self) == tokenized(other), s"$self\n\nwas not the same Hql as:\n\n$other")
  }
}
