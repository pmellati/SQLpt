package sqlpt.ast.statements

object Statements {
  case class Statements(statements: Seq[Statement]) {
    def ~(statement: Statement) =
      copy(statements = statements :+ statement)

    def ~(other: Statements) =
      copy(statements = statements ++ other.statements)
  }

  implicit def statementToStatements(statement: Statement): Statements =
    Statements(Seq(statement))

  def statements(s: Statements, ss: Statements*): Statements =
    (s +: ss).reduce(_ ~ _)
}
