package sqlpt.examples

import sqlpt.api._

object partitioned_and_unpartitioned_insert {
  object Employees extends TableDef {
    override def name = "mydb.employees"

    case class Columns(
      age:   Column[Num],
      id:    Column[Str],
      name:  Column[Str],

      year:  PartitioningColumn[Num],
      month: PartitioningColumn[Num],
      day:   PartitioningColumn[Num]
    )
  }

  object Games extends TableDef {
    override def name = "games"

    case class Columns(
      name:  Column[Str],
      genre: Column[Str]
    )
  }

  Employees.table.insert(
    Employees.table.select {employee =>
      Employees.Columns(
        id    = employee.id,
        name  = employee.name,
        age   = employee.age,

        year  = partition(employee.age),
        month = partition(10),
        day   = partition(20)
      )
    }
  )

  Games.table.insert(
    Games.table.select(identity)
  ).overwrite()
}