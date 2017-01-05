package sqlpt

import org.specs2.mutable.Specification
import org.specs2.matcher.{Expectable, MatchResult, Matcher, NoTypedEqual}
import sqlpt.api._
import sqlpt.column.Column.SourceColumn

import reflect.runtime.universe.{Type => _, _}

class TableDefSpec extends Specification with NoTypedEqual {
  "Util.TableDef" should {
    "properly instantiate its 'Columns' case class" in {
      val generatedColumns = Games.table.cols

      generatedColumns.productArity must_== 2

      generatedColumns must beAnInstanceOf[Games.Columns]

      generatedColumns.name  must beColumn(tableName = "db.games", columnName = "overriden_name")
      generatedColumns.score must beColumn(tableName = "db.games", columnName = "score")
    }
  }

  private def beColumn[T <: Type : TypeTag](tableName: String, columnName: String) = hackyMatcher[Column[T]] {c =>
    c must beAnInstanceOf[SourceColumn[T]]
    val sc = c.asInstanceOf[SourceColumn[T]]

    sc.tableName must_== tableName
    sc.name      must_== columnName
    (sc.columnTypeTag.tpe =:= typeOf[T]) must_== true
  }

  object Games extends TableDef with NoPartitioning {
    override def name = "db.games"

    case class Columns(
      @Named("overriden_name") name: Column[Str],
      score: Column[Num]
    )
  }

  private def hackyMatcher[T](res: T => MatchResult[Any]): Matcher[T] = new Matcher[T] {
    override def apply[S <: T](t: Expectable[S]): MatchResult[S] =
      res(t.value).asInstanceOf[MatchResult[S]]
  }

  "Util.TableDef.columnTypeToTypeTag(...)" should {
    "Handle all column types" in {
      import Type._

      def mustHandle[T <: Type : TypeTag] =
        Util.TableDef.columnTypeToTypeTag(typeOf[T]).tpe =:= typeTag[T].tpe must_== true

      mustHandle[Str]
      mustHandle[Num]
      mustHandle[Bool]
      mustHandle[Nullable[Str]]
      mustHandle[Nullable[Num]]
      mustHandle[Nullable[Bool]]
    }
  }
}
