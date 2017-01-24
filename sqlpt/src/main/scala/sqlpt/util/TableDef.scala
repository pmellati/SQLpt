package sqlpt.util

import sqlpt.column._, Column._
import sqlpt.ast.expressions.Table, Table.Partitioning.{Partitioned, Unpartitioned}
import shapeless._, labelled.{FieldType, field}
import annotation.implicitNotFound
import reflect.runtime.universe.TypeTag

trait TableDef {
  import TableDef._

  def name: String

  type Columns      <: Product
  type Partitioning <: Table.Partitioning

  def table(
    implicit
    columnsInst:      ColumnsProductInstantiator[Columns],
    partitioningInst: PartitioningInstantiator[Partitioning]
  ): Table[Columns, Partitioning] =
    Table(name, columnsInst.columns(name), partitioningInst.partitioning(name))
}

object TableDef {
  @implicitNotFound(msg = "${T} seems to be an invalid product of columns.")
  trait ColumnsProductInstantiator[T] {
    def columns(tableName: String): T
  }

  object ColumnsProductInstantiator {
    def instantiator[A](inst: String => A) = new ColumnsProductInstantiator[A] {
      def columns(tableName: String): A = inst(tableName)
    }

    implicit val hNillColsProdInstantiator: ColumnsProductInstantiator[HNil] = instantiator {_ => HNil}

    implicit def hListColsProdInstantiator[K <: Symbol, H, T <: HList](
      implicit
      fieldSymbol: Witness.Aux[K],
      headColInst: Lazy[SingleColumnInstantiator[H]],
      tailInst:    ColumnsProductInstantiator[T]
    ): ColumnsProductInstantiator[FieldType[K, H] :: T] = instantiator {tableName =>
      val fieldName = fieldSymbol.value.name

      val column = headColInst.value.column(tableName, fieldName)

      field[K](column) :: tailInst.columns(tableName)
    }

    implicit def genericColsProdInstantiator[A, H <: HList](
      implicit
      generic:   LabelledGeneric.Aux[A, H],
      hlistInst: Lazy[ColumnsProductInstantiator[H]]
    ): ColumnsProductInstantiator[A] = instantiator {tableName =>
      generic from hlistInst.value.columns(tableName)
    }
  }

  trait SingleColumnInstantiator[T] {
    def column(tableName: String, columnName: String): T
  }

  object SingleColumnInstantiator {
    implicit def singleColumnInstantiator[T <: Type : TypeTag]: SingleColumnInstantiator[Column[T]] =
      new SingleColumnInstantiator[Column[T]] {
        def column(tableName: String, columnName: String): Column[T] =
          SourceColumn(tableName, columnName)
      }
  }

  @implicitNotFound(msg = "${P} seems to be an invalid 'Table.Partitioning'.")
  trait PartitioningInstantiator[P <: Table.Partitioning] {
    def partitioning(tableName: String): P
  }

  object PartitioningInstantiator {
    implicit object UnpartitionedPartitioningInstantiator extends PartitioningInstantiator[Unpartitioned] {
      override def partitioning(tableName: String) = Unpartitioned
    }

    implicit def partitionedPartitioningInstantiator[P <: Product : ColumnsProductInstantiator]:
    PartitioningInstantiator[Partitioned[P]] = new PartitioningInstantiator[Partitioned[P]] {
      override def partitioning(tableName: String) =
        Partitioned(implicitly[ColumnsProductInstantiator[P]].columns(tableName))
    }
  }
}
