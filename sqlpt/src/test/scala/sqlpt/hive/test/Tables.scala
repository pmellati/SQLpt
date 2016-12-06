package sqlpt.hive.test

import hivevalid.HiveValid
import sqlpt.api._

object Tables {
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

  trait TablesEnv {
    HiveValid.runMeta("""CREATE DATABASE IF NOT EXISTS db""")

    HiveValid.runMeta(s"""
      |CREATE TABLE IF NOT EXISTS ${Cars.name} ( car_id String, model String, manufacturer_id String, price int, website String )
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY '\t'
      |LINES TERMINATED BY '\n'
      |STORED AS TEXTFILE
    """.stripMargin)
  }
}
