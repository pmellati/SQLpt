package sqlpt

import scala.reflect.macros.blackbox.Context
import ColumnTypeValidation._

/** Used as evidence that 'T' is a valid selection of columns. */
trait Columns[T]

object ColumnsTypeChecking {
  import ColumnsTypeCheckingLogic._

  /** Will fail compilation altogether if 'T' is not a valid type. Used as a type checking mechanism. */
  implicit def typeCheck[T]: Columns[T] =
    macro typeCheck_impl[T]

  def typeCheck_impl[Cols : c.WeakTypeTag](c: Context): c.Expr[Columns[Cols]] = {
    import c.universe._

    columnTypeValidationOf(c) match {
      case Valid() =>
        c.Expr[Columns[Cols]](q"""new Columns[${weakTypeOf[Cols]}] {}""")

      case Invalid(msg) =>
        c.abort(c.enclosingPosition, s"Typechecking of columns failed: $msg")
    }
  }

  /**
    * The total function version of 'typeCheck', as it won't fail compilation, but instead simply return
    * a validation outcome.
    *
    * Used for testing of the type checking logic.
    */
  implicit def reifyValidation[T]: ColumnTypeValidation[T] =
    macro reifyValidation_impl[T]

  def reifyValidation_impl[Cols : c.WeakTypeTag](c: Context): c.Expr[ColumnTypeValidation[Cols]] = {
    import c.universe._

    columnTypeValidationOf(c) match {
      case Valid() =>
        c.Expr[ColumnTypeValidation[Cols]](q"""ColumnTypeValidation.Valid()""")
      case Invalid(msg) =>
        c.Expr[ColumnTypeValidation[Cols]](q"""ColumnTypeValidation.Invalid($msg)""")
    }
  }
}

sealed trait ColumnTypeValidation[T] {
  def +(other: ColumnTypeValidation[Any]): ColumnTypeValidation[T] = (this, other) match {
    case (Valid(),       Valid())       => Valid()
    case (Invalid(msg),  Valid())       => Invalid(msg)
    case (Valid(),       Invalid(msg))  => Invalid(msg)
    case (Invalid(msg1), Invalid(msg2)) => Invalid(s"$msg1\n\n$msg2")
  }

  def mapInvalidMsg(f: String => String): ColumnTypeValidation[T] = this match {
    case Valid()      => Valid()
    case Invalid(msg) => Invalid(f(msg))
  }
}

object ColumnTypeValidation {
  case class Valid  [T]()            extends ColumnTypeValidation[T]
  case class Invalid[T](msg: String) extends ColumnTypeValidation[T]
}

object ColumnsTypeCheckingLogic {
  // TODO: Rethink the error messages and their reporting style.
  def columnTypeValidationOf[Cols : c.WeakTypeTag](c: Context): ColumnTypeValidation[Cols] = {
    import c.universe._

    object Helpers {
      val tupleTypes = Seq(
        typeOf[Tuple1[_]],
        typeOf[(_, _)],
        typeOf[(_, _, _)],
        typeOf[(_, _, _, _)],
        typeOf[(_, _, _, _, _)],
        typeOf[(_, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _, _, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)],
        typeOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)]
      )

      implicit class TypeOps(t: Type) {
        def isIn(ts: Iterable[Type]): Boolean =
          ts.exists(t <:< _)

        def isTuple =
          t isIn tupleTypes

        def isCaseClass =
          t.typeSymbol.isClass && t.typeSymbol.asClass.isCaseClass

        def caseClassCtorTypes: List[Type] =
          t.member(termNames.CONSTRUCTOR).asMethod.paramLists.head.map {_.typeSignature}
      }

      def combinedValidationsOf(types: List[Type]) =
        types.foldLeft[ColumnTypeValidation[Cols]](Valid()){(acc, componentType) =>
          acc + columnTypeValidationOf(c)(c.WeakTypeTag(componentType))
        }
    }

    import Helpers._

    val colsT = weakTypeOf[Cols]

    if(colsT <:< typeOf[Column[_ <: Column.Type]])
      Valid()
    else if (colsT.isTuple) {
      combinedValidationsOf(colsT.typeArgs).mapInvalidMsg {msg =>
        s"Invalid tuple component type(s):\n\n$msg"
      }
    } else if (colsT.isCaseClass) {
      val ctorTypes = colsT.caseClassCtorTypes

      if(ctorTypes.isEmpty)
        Invalid("Case class with no parameters is not allowed.")
      else
        combinedValidationsOf(ctorTypes).mapInvalidMsg {msg =>
          s"Invalid case class component type(s):\n\n$msg"
        }
    } else
      Invalid(s"'$colsT' is not a valid 'Columns'.")
  }
}