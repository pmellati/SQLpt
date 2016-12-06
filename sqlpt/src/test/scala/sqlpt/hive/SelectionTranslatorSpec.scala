package sqlpt.hive

import org.specs2.mutable.Specification
import org.specs2.matcher.NoTypedEqual
import sqlpt.api._
import sqlpt.hive.{SelectionTranslator => translate}
import test.hivevalid.HiveValid
import TestingTables._

class SelectionTranslatorSpec extends Specification with NoTypedEqual with test.Helpers {
  "SelectionTranslator" should {
    "successfully translate a 'SimpleSelection'" in {
      HiveValid.runMeta("""CREATE DATABASE IF NOT EXISTS db""")

      HiveValid.runMeta(s"""
        |CREATE TABLE IF NOT EXISTS ${Cars.name} ( car_id String, model String, manufacturer_id String, price int, website String )
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY '\t'
        |LINES TERMINATED BY '\n'
        |STORED AS TEXTFILE
      """.stripMargin)

      translate(Cars.table.select(_.make)) must beSameHqlAs("""
        |SELECT A.manufacturer_id
        |FROM   db.cars A
      """.stripMargin)
    }
  }
}

object TestingTables {
  object Cars extends TableDef {
    override def name = "db.cars"

    case class Columns(
      id:      Column[Str]           = "car_id",
      model:   Column[Str]           = "model",
      make:    Column[Str]           = "manufacturer_id",
      price:   Column[Num]           = "price",
      website: Column[Nullable[Str]] = "website"
    )

    override def cols = Columns()
  }
}