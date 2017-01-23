package sqlpt

import column._
import Column._
import Type._
import Arithmetic._
import util.TableDef
import ColumnsTypeChecking._
import ColumnTypeValidation._
import org.specs2.matcher.NoTypedEqual
import org.specs2.mutable
import sqlpt.api.NoPartitioning

// TODO: Tests may not be comprehensive.
class ColumnsTypeCheckingSpec extends mutable.Specification with NoTypedEqual {
  "'Columns' type checking rules" should {
    "accept individual columns" in {
      validationOf[Column[Str]] must_== Valid()

      validationOf[Column[Num]] must_== Valid()

      validationOf[Multiplication] must_== Valid()

      validationOf[String] must_== Invalid("'String' is not a valid 'Columns'.")
    }

    "accept tuples of acceptable types" in {
      validationOf[(Multiplication, SourceColumn[Str])] must_== Valid()

      validationOf[(Multiplication, (SourceColumn[Str], And))] must_== Valid()

      validationOf[(Multiplication, String, SourceColumn[Str])] must_== Invalid(
        """|Invalid tuple component type(s):
           |
           |'String' is not a valid 'Columns'.""".stripMargin)

      validationOf[(Column[Num], Cars.Columns)] must_== Valid()
    }

    "accept non-empty case classes made-up of acceptable types" in {
      validationOf[(Cars.Columns)] must_== Valid()

      case class CaseClassWithInvalidTypes(price: Column[Str], nonColumnCrap: Int)

      validationOf[CaseClassWithInvalidTypes] must_== Invalid(
        """|Invalid case class component type(s):
          |
          |'Int' is not a valid 'Columns'.""".stripMargin)

      case class EmptyCaseClass()

      validationOf[EmptyCaseClass] must_== Invalid("Case class with no parameters is not allowed.")
    }

    "not accept types that are neither 'Column[T]', nor tuples and nor case classes" in {
      class NormalClass
      trait Trait

      validationOf[NormalClass] must_== Invalid("'NormalClass' is not a valid 'Columns'.")

      validationOf[Trait] must_== Invalid("'Trait' is not a valid 'Columns'.")
    }
  }

  object Cars extends TableDef with NoPartitioning {
    override val name = "cars"

    case class Columns(
      id:    Column[Str],
      price: Column[Num]
    )
  }

  private def validationOf[Cols: ColumnTypeValidation] =
    implicitly[ColumnTypeValidation[Cols]]
}
