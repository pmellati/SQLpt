package sqlpt.util

import java.util.UUID.randomUUID

import sqlpt.ast.expressions.{Selection, Table}
import sqlpt.ast.statements._, Statements._

object Misc extends Insertion.Implicits {
  def withTempTable[Cols <: Product, R]
  (selection: Selection[Cols])(action: (=> Table[Cols]) => Statements): Statements = {
    val uniqueTempTableName =
      "sqlpt_temp_table_" + randomUUID

    val tempTable = Table(uniqueTempTableName, selection.cols)

    statements(
      // TODO: There may be existing code in tests to generate table DDL from `Table` instances.
      StringStatement(s"""
        |CREATE TALBE $uniqueTempTableName
        |ROW FORMAT DELIMITED FIELDS
        |TERMINATED BY '|'
        |STORED AS TEXTFILE
      """.stripMargin),

      tempTable.insert(selection),

      action(tempTable),

      StringStatement(s"""
        |DROP TABLE IF EXISTS $uniqueTempTableName
      """.stripMargin)
    )
  }
}
