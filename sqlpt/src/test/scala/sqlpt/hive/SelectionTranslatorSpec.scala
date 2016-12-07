package sqlpt.hive

import hivevalid.Matchers._
import org.specs2.mutable.Specification
import org.specs2.matcher.NoTypedEqual
import sqlpt.api._
import sqlpt.hive.{SelectionTranslator => translate}
import test.Tables._

class SelectionTranslatorSpec extends Specification with NoTypedEqual with TablesEnv {
  "SelectionTranslator" should {
    "successfully translate a 'SimpleSelection'" in {
      translate(Cars.table.select(_.make)) must beSameHqlAs("""
        |SELECT A.manufacturer_id
        |FROM   db.cars A
      """.stripMargin)
    }
  }
}