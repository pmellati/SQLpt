package sqlpt.util

import sqlpt.ast.expressions.Table

trait PartitioningDef {this: TableDef =>
  type Partition <: Product

  override type Partitioning = Table.Partitioning.Partitioned[Partition]
}

trait NoPartitioning {this: TableDef =>
  override type Partitioning = Table.Partitioning.Unpartitioned
}
