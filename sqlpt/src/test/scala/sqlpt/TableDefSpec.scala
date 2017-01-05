package sqlpt

import org.specs2.mutable.Specification
import org.specs2.matcher.{Expectable, MatchersCreation, MatchResult, Matcher, NoTypedEqual}
import sqlpt.api._
import sqlpt.column.Column.SourceColumn

import reflect.runtime.universe.{Type => ReflectType, _}

class TableDefSpec extends Specification with NoTypedEqual with MatchersCreation {
  "Util.TableDef" should {
    "properly instantiate its 'Columns' case class" in {
      val generatedColumns = Games.table.cols

      generatedColumns.productArity must_== 3

      generatedColumns must beAnInstanceOf[Games.Columns]

      generatedColumns.name       must beColumn(tableName = "db.games", columnName = "overriden_name")
      generatedColumns.score      must beColumn(tableName = "db.games", columnName = "score")
      generatedColumns.isReleased must beColumn(tableName = "db.games", columnName = "isReleased")
    }
  }

  object Games extends TableDef with NoPartitioning {
    override def name = "db.games"

    case class Columns(
      @ColumnName("overriden_name")
      name:       Column[Str],
      score:      Column[Nullable[Num]],
      isReleased: Column[Bool]
    )
  }

  private def beColumn[T <: Type : TypeTag](tableName: String, columnName: String) = hackyMatcher[Column[T]] {c =>
    c must beAnInstanceOf[SourceColumn[T]]
    val sc = c.asInstanceOf[SourceColumn[T]]

    sc.tableName must_== tableName
    sc.name      must_== columnName

    sc.columnTypeTag.tpe must beSameReflectTypeAs (typeOf[T])
  }

  def beSameReflectTypeAs(t2: ReflectType): Matcher[ReflectType] = {t1: ReflectType =>
    (t1 =:= t2, s"Type $t1 wasn't the same as Type $t2.")
  }

  private def hackyMatcher[T](res: T => MatchResult[Any]): Matcher[T] = new Matcher[T] {
    override def apply[S <: T](t: Expectable[S]): MatchResult[S] =
      res(t.value).asInstanceOf[MatchResult[S]]
  }
}
