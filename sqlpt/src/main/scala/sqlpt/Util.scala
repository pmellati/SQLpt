package sqlpt

import java.util.UUID.randomUUID

import sqlpt.column._, Column._, Type._
import sqlpt.ast.expressions.{Selection, Table}, Table.Partitioning.Unpartitioned
import sqlpt.ast.statements.Statements._
import sqlpt.ast.statements.StringStatement
import sqlpt.ast.statements.Insertion

import annotation.StaticAnnotation
import reflect.runtime.universe.{Type => ReflectType, Mirror => _, _}
import reflect.api._

object Util extends Insertion.Implicits {
  def withTempTable[Cols <: Product, R]
  (selection: Selection[Cols])(action: (=> Table[Cols, Unpartitioned]) => Statements): Statements = {
    val uniqueTempTableName =
      "sqlpt_temp_table_" + randomUUID

    val tempTable = Table(uniqueTempTableName, selection.cols, Unpartitioned)

    statements(
      // TODO: There may be existing code in tests to generate table DDL from `Table` instances.
      StringStatement(s"""
        |CREATE TALBE $uniqueTempTableName
        |ROW FORMAT DELIMITED FIELDS
        |TERMINATED BY '|'
        |STORED AS TEXTFILE
      """.stripMargin),

      tempTable.insert(selection),

      action(tempTable),

      StringStatement(s"""
        |DROP TABLE IF EXISTS $uniqueTempTableName
      """.stripMargin)
    )
  }

  trait TableDef {
    import TableDef._

    type Columns      <: Product
    type Partitioning <: Table.Partitioning

    def name:         String
    def partitioning: Partitioning  // TODO: Should become a private def, like 'instantiateColumnsReflectively'.

    final def table(implicit ctt: TypeTag[Columns]) = Table(name, instantiateColumnsReflectively, partitioning)

    protected type ColumnName = Annotations.ColumnName

    private def instantiateColumnsReflectively(implicit ctt: TypeTag[Columns]): Columns = {
      val typ = typeOf[Columns]

      val tableDefClassLoaderMirror = runtimeMirror(this.getClass.getClassLoader)

      /** TODO: Replace with a compile-time check.
        * See: http://stackoverflow.com/questions/30233178/how-to-check-if-some-t-is-a-case-class-at-compile-time-in-scala
        */
      if (!typ.typeSymbol.isClass || !typ.typeSymbol.asClass.isCaseClass)
        throw new RuntimeException(s"${typ.typeSymbol.fullName} is not a case class.")

      val ctor = typ.typeSymbol.asClass.typeSignature.members.filter(_.isConstructor).head.asMethod

      val params = ctor.paramLists.head

      val columns = params.map {param =>
        val columnName = param.annotations.find {
          _.tree.tpe <:< typeOf[ColumnName]}
        .map {
          /** See: [[scala.reflect.api.Annotations]] */
          _.tree.children.tail.head.asInstanceOf[Literal].value.value.toString
        } getOrElse
          param.name.toString

        val columnTypeTag = typeToTypeTag(param.typeSignature.typeArgs.head, tableDefClassLoaderMirror)

        SourceColumn(tableName, columnName)(columnTypeTag)
      }

      val tableDefInstanceMirror = tableDefClassLoaderMirror.reflect(this)
      val columnsClassMirror = tableDefInstanceMirror.reflectClass(typ.typeSymbol.asClass)
      columnsClassMirror.reflectConstructor(ctor).apply(columns: _*).asInstanceOf[Columns]
    }

    private def tableName = name

    // Based on: http://stackoverflow.com/questions/27887386/get-a-typetag-from-a-type
    private def typeToTypeTag(
      tpe: ReflectType,
      mirror: Mirror[reflect.runtime.universe.type]
    ): TypeTag[_ <: Column.Type] =
      TypeTag(mirror, new TypeCreator {
        def apply[U <: Universe with Singleton](m: Mirror[U]) =
          if (m eq mirror) tpe.asInstanceOf[U # Type]
          else sys.error(s"Type tag defined in $mirror cannot be migrated to other mirrors.")
      })
  }

  object TableDef {
    object Annotations {
      class ColumnName(val columnName: String) extends StaticAnnotation
    }
  }

  trait PartitioningDef {
    protected type PartitionKey[T <: Type] = PartitionCol[T]

    protected implicit def str2PartitionKey[T <: Type : TypeTag](keyName: String): PartitionCol[T] =
      PartitionCol.Key(keyName)
  }

  trait NoPartitioning extends PartitioningDef {this: TableDef =>
    override type Partitioning = Table.Partitioning.Unpartitioned
    override def partitioning = Table.Partitioning.Unpartitioned
  }
}
