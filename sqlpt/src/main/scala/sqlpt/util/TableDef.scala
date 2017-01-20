package sqlpt.util

import sqlpt.column._, Column._, Type._
import sqlpt.ast.expressions.Table
import shapeless._, labelled.{FieldType, field}
import annotation.implicitNotFound

trait TableDef {
  import TableDef._

  type Columns      <: Product
  type Partitioning <: Table.Partitioning

  def name:         String
  def partitioning: Partitioning

  def cols(implicit colsCr: ColumnsProductInstantiator[Columns]): Columns =
    colsCr.instantiate(name)

  def table(implicit colsCr: ColumnsProductInstantiator[Columns]) =
    Table(name, cols, partitioning)
}

object TableDef {
  @implicitNotFound(msg = "Couldn't figure out how to instantiate a ${T}.\nIs it a valid product of columns?")
  trait ColumnsProductInstantiator[T] {
    def instantiate(tableName: String): T
  }

  object ColumnsProductInstantiator {
    def instantiator[A](inst: String => A) = new ColumnsProductInstantiator[A] {
      def instantiate(tableName: String): A = inst(tableName)
    }

    trait Implicits {
      implicit val hNillColsProdInstantiator: ColumnsProductInstantiator[HNil] = instantiator {_ => HNil}

      implicit def hListColsProdInstantiator[K <: Symbol, H, T <: HList](
        implicit
        fieldSymbol: Witness.Aux[K],
        headColInst: Lazy[SingleColumnInstantiator[H]],
        tailInst:    ColumnsProductInstantiator[T]
      ): ColumnsProductInstantiator[FieldType[K, H] :: T] = instantiator {tableName =>
        val fieldName = fieldSymbol.value.name

        val column = headColInst.value.instantiate(tableName, fieldName)

        field[K](column) :: tailInst.instantiate(tableName)
      }

      implicit def genericColsProdInstantiator[A, H <: HList](
        implicit
        generic:   LabelledGeneric.Aux[A, H],
        hlistInst: Lazy[ColumnsProductInstantiator[H]]
      ): ColumnsProductInstantiator[A] = instantiator {tableName =>
        generic from hlistInst.value.instantiate(tableName)
      }
    }
  }

  trait SingleColumnInstantiator[T] {
    def instantiate(tableName: String, columnName: String): T
  }

  object SingleColumnInstantiator {
    def instantiator[A](inst: (String, String) => A) = new SingleColumnInstantiator[A] {
      def instantiate(tableName: String, columnName: String): A = inst(tableName, columnName)
    }

    trait Implicits {
      // TODO: Can we have a single def instead of all these?

      implicit val strColInst = instantiator[Column[Str]] {case (tableName, colName) =>
        SourceColumn(tableName, colName)
      }

      implicit val boolColInst = instantiator[Column[Bool]] {case (tableName, colName) =>
        SourceColumn(tableName, colName)
      }

      implicit val nullableNumColInst = instantiator[Column[Nullable[Num]]] {case (tableName, colName) =>
        SourceColumn(tableName, colName)
      }
    }
  }
}
