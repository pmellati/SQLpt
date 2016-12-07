package sqlpt.hive

import org.specs2.mutable.Spec
import sqlpt.api._
import HqlWriter.toHql
import hivevalid.Matchers._
import test.Tables._

class SelfJoinAffinitySpec extends Spec with TablesEnv {
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
        |FROM db.cars A
        |JOIN db.cars B
        |ON A.car_id = B.car_id
        |WHERE B.price = 12000.0
      """.stripMargin)
  }
}
