package sqlpt.util

import sqlpt.column._, Column._
import sqlpt.ast.expressions.Table

import reflect.runtime.universe.TypeTag

trait PartitioningDef {
  protected type PartitionKey[T <: Type] = PartitionCol[T]

  protected implicit def str2PartitionKey[T <: Type : TypeTag](keyName: String): PartitionCol[T] =
    PartitionCol.Key(keyName)
}

trait NoPartitioning extends PartitioningDef {this: TableDef =>
  override type Partitioning = Table.Partitioning.Unpartitioned
  override def partitioning = Table.Partitioning.Unpartitioned
}
