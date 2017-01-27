package sqlpt.util

import sqlpt.column._, Column._
import sqlpt.ast.expressions.Table
import shapeless._, labelled.{FieldType, field}
import annotation.implicitNotFound
import reflect.runtime.universe.TypeTag

trait TableDef {
  import TableDef._

  def name: String

  type Columns <: Product

  def fieldNameToColumnName: FieldNameToColName = identity

  def table(implicit columnsInst: ColumnsProductInstantiator[Columns]): Table[Columns] =
    Table(name, columnsInst.columns(name, fieldNameToColumnName))
}

object TableDef {
  type FieldNameToColName = String => String

  sealed trait PartitioningColumnTag
  type PartitioningColumn[+T <: Type] = Column[T] with PartitioningColumnTag

  @implicitNotFound(msg = "${T} seems to be an invalid product of columns.")
  trait ColumnsProductInstantiator[T] {
    def columns(tableName: String, fieldNameToColumnName: FieldNameToColName): T
  }

  object ColumnsProductInstantiator {
    def instantiator[A](inst: (String, FieldNameToColName) => A) = new ColumnsProductInstantiator[A] {
      def columns(tableName: String, f2c: FieldNameToColName): A = inst(tableName, f2c)
    }

    implicit val hNillColsProdInstantiator: ColumnsProductInstantiator[HNil] = instantiator {(_, _) => HNil}

    implicit def hListColsProdInstantiator[K <: Symbol, H, T <: HList](
      implicit
      fieldSymbol: Witness.Aux[K],
      headColInst: Lazy[SingleColumnInstantiator[H]],
      tailInst:    ColumnsProductInstantiator[T]
    ): ColumnsProductInstantiator[FieldType[K, H] :: T] = instantiator {case (tableName, fieldNameToColumnName) =>
      val fieldName = fieldSymbol.value.name

      val column = headColInst.value.column(tableName, fieldNameToColumnName(fieldName))

      field[K](column) :: tailInst.columns(tableName, fieldNameToColumnName)
    }

    implicit def genericColsProdInstantiator[A, H <: HList](
      implicit
      generic:   LabelledGeneric.Aux[A, H],
      hlistInst: Lazy[ColumnsProductInstantiator[H]]
    ): ColumnsProductInstantiator[A] = instantiator {case (tableName, f2c) =>
      generic from hlistInst.value.columns(tableName, f2c)
    }
  }

  trait SingleColumnInstantiator[T] {
    def column(tableName: String, columnName: String): T
  }

  object SingleColumnInstantiator {
    implicit def singleColumnInstantiator[T <: Type : TypeTag]: SingleColumnInstantiator[Column[T]] =
      new SingleColumnInstantiator[Column[T]] {
        def column(tableName: String, columnName: String): Column[T] =
          SourceColumn(tableName, columnName, isPartitioning = false)
      }

    implicit def singlePartitioningColInstantiator[T <: Type : TypeTag]: SingleColumnInstantiator[PartitioningColumn[T]] =
      new SingleColumnInstantiator[PartitioningColumn[T]] {
        def column(tableName: String, columnName: String): PartitioningColumn[T] =
          SourceColumn(tableName, columnName, isPartitioning = true).asInstanceOf[PartitioningColumn[T]]
      }
  }
}
