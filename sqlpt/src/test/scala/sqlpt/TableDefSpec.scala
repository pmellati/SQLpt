package sqlpt

import org.specs2.mutable.Specification
import org.specs2.matcher.NoTypedEqual
import sqlpt.api._

class TableDefSpec extends Specification with NoTypedEqual{
  "Util.TableDef" should {
    "properly instantiate its 'Columns' case class" in {
      Games.table.cols must beAnInstanceOf[Games.Columns]

      // TODO: Check the columns and their type tags.
    }
  }

  object Games extends TableDef with NoPartitioning {
    override def name = "db.games"

    case class Columns(
      @Named("dsf") name: Column[Str],
      score: Column[Num]
    )
  }
}

