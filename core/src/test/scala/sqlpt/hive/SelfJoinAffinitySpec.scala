package sqlpt.hive

import org.specs2.mutable.Spec
import org.specs2.matcher._
import sqlpt.api._
import HqlWriter.{Hql, toHql}

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

  private def beSameHqlAs(other: Hql): Matcher[Hql] = {self: Hql =>
    def tokenized(text: Hql) =
      text.split("\\s").toSeq.filterNot(_.trim.isEmpty)

    (tokenized(self) == tokenized(other), s"$self\n\nwas not the same Hql as:\n\n$other")
  }
}
