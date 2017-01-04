//package sqlpt.hive
//
//import hivevalid.Matchers._
//import org.specs2.mutable.Specification
//import org.specs2.matcher.{Matcher, NoTypedEqual}
//import sqlpt.api._
//import test.Tables._
//
//// TODO: These tests need better organization.
//class SelectionTranslatorSpec extends Specification with NoTypedEqual with TablesEnv {
//  "SelectionTranslator" should {
//    "successfully translate a 'SimpleSelection' without joins" in {
//      Cars.table.select(_.make) must translateTo("""
//        |SELECT A.manufacturer_id
//        |FROM   db.cars A
//      """.stripMargin)
//
//      Cars.table.select(car => (
//        car.id,
//        car.model,
//        car.price)
//      ) must translateTo("""
//        |SELECT A.car_id, A.model, A.price
//        |FROM   db.cars A
//      """.stripMargin)
//
//      Cars.table.select(car => (
//        car.model,
//        car.id,
//        car.model,
//        car.model,
//        car.price)
//      ) must translateTo("""
//        |SELECT A.car_id, A.model, A.price
//        |FROM   db.cars A
//      """.stripMargin)
//
//      Cars.table.selectDistinct(car => (
//        car.id,
//        car.model,
//        car.price)
//      ) must translateTo("""
//        |SELECT DISTINCT A.car_id, A.model, A.price
//        |FROM   db.cars A
//      """.stripMargin)
//
//      Cars.table
//        .where(_.price >= 20000)
//        .select(car => (
//          car.id,
//          car.model,
//          car.price)
//      ) must translateTo("""
//        |SELECT A.car_id, A.model, A.price
//        |FROM   db.cars A
//        |WHERE  A.price >= 20000.0
//      """.stripMargin)
//
//      Seq(
//        Cars.table
//          .where(car =>
//            car.price >= 20000   and
//            car.make === "Fiat")
//          .select(car => (
//            car.id,
//            car.model,
//            car.price)
//          ),
//        Cars.table
//          .where(_.price >= 20000)
//          .where(_.make === "Fiat")
//          .select(car => (
//            car.id,
//            car.model,
//            car.price)
//          )
//      ) must contain(
//        translateTo("""
//          |SELECT A.car_id, A.model, A.price
//          |FROM   db.cars A
//          |WHERE  A.price >= 20000.0 AND A.manufacturer_id = "Fiat"
//        """.stripMargin)
//      ).forall
//
//      Seq(
//        Cars.table.select(identity),
//        Cars.table.select(car => (
//          car,
//          car.model,
//          car.price)
//        )
//      ) must contain(
//        translateTo("""
//          |SELECT A.car_id, A.manufacturer_id, A.model, A.price, A.website
//          |FROM   db.cars A
//        """.stripMargin)
//      ).forall
//
//      case class ModelAndPrice(model: Column[Str], price: Column[Num])
//
//      Cars.table
//        .select(c =>
//          ModelAndPrice(c.model, c.price)
//        ) must translateTo("""
//          |SELECT A.model, A.price
//          |FROM   db.cars A
//        """.stripMargin)
//
//      Cars.table
//        .select(c => (
//          ModelAndPrice(c.model, c.price),
//          c.price,
//          c.website
//        )) must translateTo("""
//          |SELECT A.model, A.price, A.website
//          |FROM   db.cars A
//        """.stripMargin)
//    }
//
//    // TODO: Test other joins.
//    "successfully translate a 'SimpleSelection' with joins" in {
//      Cars.table
//        .join(Cars.table) {_.model === _.make}
//        .where  {case (_, c2) => c2.price >= 1000}
//        .select {case (c1, c2) => (
//          c1,
//          c2.make,
//          c2.price
//        )} must translateTo("""
//          |SELECT A.car_id, A.manufacturer_id, A.model, A.price, A.website, B.manufacturer_id, B.price
//          |FROM  db.cars A
//          |JOIN  db.cars B ON A.model = B.manufacturer_id
//          |WHERE B.price >= 1000.0
//        """.stripMargin)
//
//      Cars.table
//        .leftJoin(Cars.table) {_.model === _.make}
//        .where  {case (c1, _) => c1.price >= 1000}
//        .select {case (c1, c2) => (
//          c1.id,
//          c2(_.make),
//          c2(_.price)
//        )} must translateTo("""
//          |SELECT A.car_id, B.manufacturer_id, B.price
//          |FROM   db.cars A
//          |LEFT JOIN  db.cars B ON A.model = B.manufacturer_id
//          |WHERE  A.price >= 1000.0
//        """.stripMargin)
//
//      Cars.table
//        .leftJoin(CarMakers.table) {_.model === _.id}
//        .where  {case (car, _) => car.price >= 1000}
//        .select {case (car, manufacturer) => (
//          car.id,
//          manufacturer(_.id),
//          manufacturer(_.numEmployees)
//        )} must translateTo("""
//          |SELECT     A.car_id, B.manufacturer_id, B.num_employees
//          |FROM       db.cars A
//          |LEFT JOIN  db.car_makers B ON A.model = B.manufacturer_id
//          |WHERE      A.price >= 1000.0
//        """.stripMargin)
//    }
//
//    // TODO: Test more cases.
//    "successfully translate a 'SimpleSelection' with subqueries" in {
//      Cars.table
//        .where(_.price === 1000)
//        .selectDistinct(c =>
//          (c.price, c.model)
//        ).where {case (price, _) =>
//          price >= 3.50
//        }.select(identity) must translateTo("""
//          |SELECT A.model, A.price
//          |FROM (
//          |  SELECT DISTINCT A.model, A.price
//          |  FROM   db.cars A
//          |  WHERE  A.price = 1000.0
//          |) A
//          |WHERE A.price >= 3.5
//        """.stripMargin)
//    }
//
//    "successfully translate a 'AggrSelection'" in {
//      Cars.table
//        .where(_.price >= 1000)
//        .groupBy(_.make)
//        .select {case (make, aggr) => (
//          make,
//          aggr.max(_.price),
//          aggr.count(_.model)
//        )}.having {case (_, _, count) =>
//          count === 14300
//        } must translateTo("""
//          |SELECT   A.manufacturer_id, COUNT(A.model), MAX(A.price)
//          |FROM     db.cars A
//          |WHERE    A.price >= 1000.0
//          |GROUP BY A.manufacturer_id
//          |HAVING   COUNT(A.model) = 14300.0
//        """.stripMargin)
//    }
//  }
//
//  private def translateTo(hql: Hql): Matcher[Selection[_ <: Product]] =
//    beSameHqlAs(hql) ^^ Translators.selection
//}