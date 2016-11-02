package sqlpt

import Statements._
import Insertion._

import java.util.UUID.randomUUID

object Util {
  def withTempTable[Cols <: Product, R](selection: Selection[Cols])(action: (=> Table[Cols]) => Statements): Statements = {
    val uniqueTempTableName =
      "sqlpt_temp_table_" + randomUUID

    statements(
      StringStatement(s"""
        |CREATE TALBE $uniqueTempTableName
        |ROW FORMAT DELIMITED FIELDS
        |TERMINATED BY '|'
        |STORED AS TEXTFILE
      """.stripMargin),

      insert(selection).into(uniqueTempTableName),

      action(Table(uniqueTempTableName, selection.cols)),

      StringStatement(s"""
        |DROP TABLE IF EXISTS $uniqueTempTableName
      """.stripMargin)
    )
  }
}
