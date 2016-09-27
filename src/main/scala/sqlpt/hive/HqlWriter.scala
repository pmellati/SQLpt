package sqlpt.hive

import sqlpt._, Column.Arithmetic._

object HqlWriter {
  type Hql = String

  def toHql(rows: Rows[_]): Hql = ???

//    rows match {
//    case selection: Selection[_, _] =>
//      val selectedColumns =
//        selection.cols.productIterator
//        .map {c => toHql(c.asInstanceOf[Column[_]])}  // TODO: We may also get a product (tuple or case class) here.
//        .mkString(", ")
//
//      val whereClause = selection.filters.reduceOption {_ and _}.fold("") {filter =>
//        s"""WHERE ${toHql(filter)}"""
//      }
//
//      val optionallyDistinct =
//        if (selection.isDistinct) "DISTINCT" else ""
//
//      s"""SELECT $optionallyDistinct $selectedColumns
//         |FROM (${toHql(selection.source)})
//         |$whereClause
//      """.stripMargin
//  }

  def toHql(column: Column[_]): Hql = ???
}
