package sqlpt.hive

import hivevalid.Matchers._
import org.specs2.mutable.Specification
import org.specs2.matcher.{Matcher, NoTypedEqual}
import sqlpt.api._
import test.TableCreator.createTable
import test.Tables._

class InsertionTranslatorSpec extends Specification with NoTypedEqual with TablesEnv {
  "The insertion translator" should {
    "translate an Insertion into an unpartitioned table" in {
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

    "translate an Insertion into a partitioned table (static partitioning)" in {
      createTable(ShippedItemsWithPartitioning.table)

      ShippedItemsWithPartitioning.table.insert(
        Cars.table.select(c => ShippedItemsWithPartitioning.Columns(
          price     = c.price,
          recipient = "Black Mesa",
          year      = partition(2004),
          batchId   = partition("myBatch")
        ))
      ) must translateTo("""
        |INSERT INTO TABLE db.shipped_items_partitioned
        |PARTITION (year="2004", batchId="myBatch")
        |SELECT "Black Mesa", A.price
        |FROM   db.shipped_items_partitioned A
      """.stripMargin)
    }
  }

  private def translateTo(hql: Hql): Matcher[Insertion] =
    beSameHqlAs(hql) ^^ Translators.insertion

  private object ShippedItems extends TableDef {
    override def name = "db.shipped_items"

    case class Columns(
      price:     Column[Num],
      recipient: Column[Str]
    )
  }

  private object ShippedItemsWithPartitioning extends TableDef {
    override def name = "db.shipped_items_partitioned"

    case class Columns(
      price:     Column[Num],
      recipient: Column[Str],

      year:    PartitioningColumn[Num],
      batchId: PartitioningColumn[Str]
    )
  }
}
