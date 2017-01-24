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
  }

  object Games extends TableDef with NoPartitioning {
    override def name = "games"

    case class Columns(
      name:  Column[Str],
      genre: Column[Str]
    )
  }

  trait PartitioningByDate extends PartitioningDef {this: TableDef =>
    case class Partition(
      year:  Column[Num],
      month: Column[Num],
      day:   Column[Num]
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
    year  = 2020,
    month = 11,
    day   = 22
  ))

  Games.table.insert(
    Games.table.select(identity)
  ).overwrite()
}