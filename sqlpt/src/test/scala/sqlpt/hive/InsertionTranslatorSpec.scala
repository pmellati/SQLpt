//package sqlpt.hive
//
//import hivevalid.{HiveValid, Matchers}, Matchers._
//import org.specs2.mutable.Specification
//import org.specs2.matcher.{Matcher, NoTypedEqual}
//import sqlpt.api._
//import test.Tables._
//
//class InsertionTranslatorSpec extends Specification with NoTypedEqual with TablesEnv {
//  "The insertion translator" should {
//    "translate an Insertion" in {
//      HiveValid.runMeta(s"""
//        |CREATE TABLE IF NOT EXISTS db.mytable ( whatevs double)
//        |ROW FORMAT DELIMITED
//        |FIELDS TERMINATED BY '\t'
//        |LINES TERMINATED BY '\n'
//        |STORED AS TEXTFILE
//      """.stripMargin)
//
//      insert(
//        Cars.table.select(_.price)
//      ).into("db.mytable") must translateTo("""
//        |INSERT INTO TABLE db.mytable
//        |SELECT A.price
//        |FROM   db.cars A
//      """.stripMargin)
//    }
//  }
//
//  private def translateTo(hql: Hql): Matcher[Insertion] =
//    beSameHqlAs(hql) ^^ Translators.insertion
//}
