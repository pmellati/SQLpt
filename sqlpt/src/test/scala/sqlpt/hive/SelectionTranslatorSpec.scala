package sqlpt.hive

import hivevalid.Matchers._
import org.specs2.mutable.Specification
import org.specs2.matcher.{Matcher, NoTypedEqual}
import sqlpt.api._
import sqlpt.hive.{SelectionTranslator => translate}
import test.Tables._

class SelectionTranslatorSpec extends Specification with NoTypedEqual with TablesEnv {
  "SelectionTranslator" should {
    "successfully translate a 'SimpleSelection'" in {
      Cars.table.select(_.make) must translateTo("""
        |SELECT A.manufacturer_id
        |FROM   db.cars A
      """.stripMargin)

      Cars.table.select(car => (
        car.id,
        car.model,
        car.price)
      ) must translateTo("""
        |SELECT A.car_id, A.model, A.price
        |FROM   db.cars A
      """.stripMargin)

      Cars.table.select(car => (
        car.model,
        car.id,
        car.model,
        car.model,
        car.price)
      ) must translateTo("""
        |SELECT A.car_id, A.model, A.price
        |FROM   db.cars A
      """.stripMargin)

      Cars.table.selectDistinct(car => (
        car.id,
        car.model,
        car.price)
      ) must translateTo("""
        |SELECT DISTINCT A.car_id, A.model, A.price
        |FROM   db.cars A
      """.stripMargin)

      Cars.table
        .where(_.price >= 20000)
        .select(car => (
          car.id,
          car.model,
          car.price)
      ) must translateTo("""
        |SELECT A.car_id, A.model, A.price
        |FROM   db.cars A
        |WHERE  A.price >= 20000.0
      """.stripMargin)

      Seq(
        Cars.table
          .where(car =>
            car.price >= 20000   and
            car.make === "Fiat")
          .select(car => (
            car.id,
            car.model,
            car.price)
          ),
        Cars.table
          .where(_.price >= 20000)
          .where(_.make === "Fiat")
          .select(car => (
            car.id,
            car.model,
            car.price)
          )
      ) must contain(
        translateTo("""
          |SELECT A.car_id, A.model, A.price
          |FROM   db.cars A
          |WHERE  A.price >= 20000.0 AND A.manufacturer_id = "Fiat"
        """.stripMargin)
      ).forall

      Seq(
        Cars.table.select(identity),
        Cars.table.select(car => (
          car,
          car.model,
          car.price)
        )
      ) must contain(
        translateTo("""
          |SELECT A.car_id, A.manufacturer_id, A.model, A.price, A.website
          |FROM   db.cars A
        """.stripMargin)
      ).forall

    }
  }

  private def translateTo(hql: Hql): Matcher[Selection[_ <: Product]] =
    beSameHqlAs(hql) ^^ translate
}