package sqlpt.hive

import org.specs2.mutable.Spec
import sqlpt._, Column._, Type._, Arithmetic._

class SelfJoinAffinitySpec extends Spec {
  object Cars extends TableDef {
    override val name = "cars"

    case class Columns(
      id:    Column[Str] = "car_id",
      price: Column[Num] = "price"
    )

    override def cols = Columns()
  }

  "Affinities should be properly determined in the translated Hql" in {
    import Literal._   // TODO: Putting this at the top of the file causes ambiguous implicit conversions.

    val query =
      Cars.table.join(Cars.table) {_.id === _.id}
        .where {case (l, r) =>
          r.price === 12000
        }.select {case (l, r) =>
          (r.id, l.id, l.price * r.price)
        }

    HqlWriter.toHql(query) must_==
      """
        |SELECT B.id, A.id, A.price * B.price
        |FROM cars A
        |JOIN cars B
        |ON A.id = B.id
        |WHERE B.price = 12000
      """.stripMargin
  }

  // TODO: Put this in the lib, so that it doesn't have to be defined everywhere.
  private implicit def rows2Filtered[Src <: Product](rows: Rows[Src]): Filtered[Src] =
    Filtered(rows, Set.empty)
}
