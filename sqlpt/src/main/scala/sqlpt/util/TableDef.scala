package sqlpt.util

import sqlpt.column._
import Column._
import sqlpt.ast.expressions.Table
import shapeless._
import labelled.{FieldType, field}
import sqlpt.util.TableDef.PartitioningColumn

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

import shapeless.ops.record._
import shapeless.ops.hlist._

object NoDup {
  trait Flattener[A] {
    type Out <: HList
  }

  object Flattener {
    type Aux[A, O <: HList] = Flattener[A] { type Out = O }

    def apply[A](implicit flattener: Flattener[A]): Aux[A, flattener.Out] = flattener


    implicit val hNilFlattener: Flattener.Aux[HNil, HNil] = null

    implicit def flattener[A, H <: HList, F <: HList](
      implicit
      lg: LabelledGeneric.Aux[A, H],
      hListFlattener: Lazy[Flattener.Aux[H, F]]
    ): Flattener.Aux[A, F] = null

    implicit def hListFlattener[H, HF <: HList, T <: HList, TF <: HList, HfTf <: HList](
      implicit
      headFlattener: Lazy[Flattener.Aux[H, HF]],
      tailFlattener: Flattener.Aux[T, TF],
      prep:          Prepend.Aux[HF, TF, HfTf]
    ): Flattener.Aux[H :: T, HfTf] = null

    implicit def swallowOuterKeyFlattener[K <: Symbol, A, Af <: HList](implicit af: Flattener.Aux[A, Af]): Flattener.Aux[FieldType[K, A], Af] = null

    implicit def columnFlattener[K <: Symbol, T <: Type : TypeTag]:
      Flattener.Aux[FieldType[K, Column[T]], FieldType[K, Column[T]] :: HNil] = null

    implicit def partitioningColumnFlattener[K <: Symbol, T <: Type : TypeTag]:
      Flattener.Aux[FieldType[K, PartitioningColumn[T]], FieldType[K, PartitioningColumn[T]] :: HNil] = null
  }

  trait HasUniqueTypes[H <: HList]

  object HasUniqueTypes {
//    implicit object hNillHasUniqueTypes extends HasUniqueTypes[HNil]


  }

  trait FlattenedKeys[A] {
    type Out <: HList
  }

  object FlattenedKeys {
    type Aux[A, O <: HList] = FlattenedKeys[A] { type Out = O }

    def apply[A](implicit fk: FlattenedKeys[A]): Aux[A, fk.Out] = fk

    implicit def flattennedKeysFromFlattener[A, Af <: HList, K <: HList](
      implicit
      fA: Flattener.Aux[A, Af],
      keys: Keys.Aux[Af, K]
    ): FlattenedKeys.Aux[A, K] = null
  }

  trait NoDuplicateFieldNames[A]

  implicit def noDupFieldNames[A, Keys <: HList](
    implicit
    flattennedKeys:        FlattenedKeys.Aux[A, Keys],
    noDuplicateFieldNames: HasUniqueTypes[Keys]
  ): NoDuplicateFieldNames[A] = null
}