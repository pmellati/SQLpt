package sqlpt.hive

import hivevalid.Matchers._
import org.specs2.mutable.Specification
import org.specs2.matcher.{Matcher, NoTypedEqual}
import sqlpt.api._
import test.TableCreator.createTable
import test.Tables._

class InsertionTranslatorSpec extends Specification with NoTypedEqual with TablesEnv {
  "The insertion translator" should {
    "translate an Insertion" in {
      createTable(ShippedItems.table)

      ShippedItems.table.insert(
        Cars.table.select(c =>
          ShippedItems.Columns(
            c.price,
            "255 Pitt St."
          )
        )
      ) must translateTo("""
        |INSERT INTO TABLE db.shipped_items
        |SELECT "255 Pitt St.", A.price
        |FROM   db.cars A
      """.stripMargin)
    }
  }

  private def translateTo(hql: Hql): Matcher[Insertion] =
    beSameHqlAs(hql) ^^ Translators.insertion

  private object ShippedItems extends TableDef with NoPartitioning {
    override def name: String = "db.shipped_items"

    case class Columns(
      price:     Column[Num],
      recipient: Column[Str]
    )
  }
}
