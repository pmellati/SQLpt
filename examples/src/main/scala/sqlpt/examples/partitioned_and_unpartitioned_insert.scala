package sqlpt.examples

import sqlpt.api._

object partitioned_and_unpartitioned_insert {
  object Employees extends TableDef with PartitioningByDate {
    override def name = "mydb.employees"

    case class Columns(
      age:  Column[Num],
      id:   Column[Str],
      name: Column[Str]
    )

    override def cols = implicit permission =>
      Columns(
        age  = col("age"),
        id   = col("employee_id"),
        name = col("name")
      )
  }

  object Games extends TableDef with NoPartitioning {
    override def name = "games"

    case class Columns(
      name:  Column[Str],
      genre: Column[Str]
    )

    override def cols = implicit permission =>
      Columns(
        name  = col("name"),
        genre = col("genre")
      )
  }

  trait PartitioningByDate extends PartitioningDef {this: TableDef =>
    case class Partition(
      year:  PartitionKey[Num],
      month: PartitionKey[Num],
      day:   PartitionKey[Num]
    )

    override type Partitioning = Table.Partitioning.Partitioned[Partition]

    override def partitioning = Table.Partitioning.Partitioned(
      Partition(
        year  = "year",
        month = "month",
        day   = "day"
      )
    )
  }

  Employees.table.insert(
    Employees.table.select {employee =>
      Employees.Columns(
        id   = employee.id,
        name = employee.name,
        age  = employee.age
      )
    }
  ).intoPartition(employee => Employees.Partition(
    year  = ???,
    month = ???,
    day   = ???
  ))

  Games.table.insert(
    Games.table.select(identity)
  ).overwrite()
}