package sqlpt

import org.specs2.mutable.Specification
import org.specs2.matcher.{Matcher, MatchersCreation, NoTypedEqual}
import sqlpt.api._
import sqlpt.column.Column.SourceColumn

import reflect.runtime.universe.{Type => ReflectType, _}

import shapeless._
import util.NoDup._

class TableDefSpec extends Specification with NoTypedEqual with MatchersCreation {
//  "Util.TableDef" should {
//    "properly instantiate its 'Columns' case class" in {
//      object Games extends Games
//
//      val generatedColumns = Games.table.cols
//
//      generatedColumns.productArity must_== 4
//
//      generatedColumns must beAnInstanceOf[Games.Columns]
//
//      generatedColumns.name       must beColumn(tableName = "db.games", columnName = "name")
//      generatedColumns.score      must beColumn(tableName = "db.games", columnName = "score")
//      generatedColumns.isReleased must beColumn(tableName = "db.games", columnName = "isReleased")
//      generatedColumns.year       must beColumn(tableName = "db.games", columnName = "year", isPartitioning = true)
//    }
//
//    "utilise its fieldNameToColumnName translation" in {
//      object GamesWithoutRenaming extends Games
//
//      GamesWithoutRenaming.table.cols.name must beColumn(tableName = "db.games", columnName = "name")
//      GamesWithoutRenaming.table.cols.year must beColumn(tableName = "db.games", columnName = "year", isPartitioning = true)
//
//      object GamesWithRenaming extends Games {
//        override def fieldNameToColumnName = _.toUpperCase
//      }
//
//      GamesWithRenaming.table.cols.name must beColumn(tableName = "db.games", columnName = "NAME")
//      GamesWithRenaming.table.cols.year must beColumn(tableName = "db.games", columnName = "YEAR", isPartitioning = true)
//    }
//  }

  "Shapeless stuff" should {
    "determine if an HList has unique types only" in {
//      the[HasUniqueTypes[HNil]]

//      the[HasUniqueTypes[Int :: String :: HNil]]

      case class Simple(name: Column[Str], age: Column[Num])

      case class Complex(price: Column[Num], simple: Simple)

      Flattener[Simple]
      Flattener[Complex]

      pending
    }
  }

  trait Games extends TableDef {
    override def name = "db.games"

    case class Columns(
      name:       Column[Str],
      score:      Column[Nullable[Num]],
      isReleased: Column[Bool],
      year:       PartitioningColumn[Num]
    )
  }

  private def beColumn[T <: Type : TypeTag](tableName: String, columnName: String, isPartitioning: Boolean = false): Matcher[Column[T]] = {c: Column[T] =>
    c must beAnInstanceOf[SourceColumn[T]]
    val sc = c.asInstanceOf[SourceColumn[T]]

    sc.tableName      must_== tableName
    sc.name           must_== columnName
    sc.isPartitioning must_== isPartitioning

    sc.columnTypeTag.tpe must beSameReflectTypeAs (typeOf[T])
  }

  private def beSameReflectTypeAs(t2: ReflectType): Matcher[ReflectType] = {t1: ReflectType =>
    // Type equivalence checking is buggy. Workaround: do 100 fake checks here to "warm up" the types.
    (1 to 100) foreach {_ => t1 =:= t2}

    (t1 =:= t2, s"Type $t1 wasn't the same as Type $t2.")
  }
}
