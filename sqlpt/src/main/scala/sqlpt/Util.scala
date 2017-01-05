package sqlpt

import java.util.UUID.randomUUID

import sqlpt.column._, Column._, Type._
import sqlpt.ast.expressions.{Selection, Table}, Table.Partitioning.Unpartitioned
import sqlpt.ast.statements.Statements._
import sqlpt.ast.statements.StringStatement
import sqlpt.ast.statements.Insertion

import scala.annotation.StaticAnnotation
import reflect.runtime.universe.{Type => _, _}

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

    protected type Named = Annotations.Named

    private def instantiateColumnsReflectively(implicit ctt: TypeTag[Columns]): Columns = {
      val typ = typeOf[Columns]

      /** TODO: Replace with a compile-time check.
        * See: http://stackoverflow.com/questions/30233178/how-to-check-if-some-t-is-a-case-class-at-compile-time-in-scala
        */
      if (!typ.typeSymbol.isClass || !typ.typeSymbol.asClass.isCaseClass)
        throw new RuntimeException(s"${typ.typeSymbol.fullName} is not a case class.")

      val ctor = typ.typeSymbol.asClass.typeSignature.members.filter(_.isConstructor).head.asMethod

      val params = ctor.paramLists.head

      val columns = params.map {param =>
        val columnName = param.annotations.find {
          _.tree.tpe <:< typeOf[Named]}
        .map {
          /** See: [[scala.reflect.api.Annotations]] */
          _.tree.children.tail.head.asInstanceOf[Literal].value.value.toString
        } getOrElse
          param.name.toString

        SourceColumn(tableName, columnName)(columnTypeToTypeTag(param.typeSignature.typeArgs.head))
      }

      val tableDefClassLoaderMirror = runtimeMirror(this.getClass.getClassLoader)
      val tableDefInstanceMirror = tableDefClassLoaderMirror.reflect(this)
      val columnsClassMirror = tableDefInstanceMirror.reflectClass(typ.typeSymbol.asClass)
      columnsClassMirror.reflectConstructor(ctor).apply(columns: _*).asInstanceOf[Columns]
    }

    private def tableName = name
  }

  object TableDef {
    object Annotations {
      class Named(val columnName: String) extends StaticAnnotation
    }

    // TODO: Is there a better way to do this? We may forget to add an entry here for newly added column types.
    protected[sqlpt] def columnTypeToTypeTag(t: reflect.runtime.universe.Type): TypeTag[_ <: Column.Type] =
      Seq(
        typeOf[Num]            -> typeTag[Num],
        typeOf[Nullable[Num]]  -> typeTag[Nullable[Num]],
        typeOf[Str]            -> typeTag[Str],
        typeOf[Nullable[Str]]  -> typeTag[Nullable[Str]],
        typeOf[Bool]           -> typeTag[Bool],
        typeOf[Nullable[Bool]] -> typeTag[Nullable[Bool]]
      ).collect {
        case (typ, typTag) if t <:< typ => typTag
      }.headOption.getOrElse(
        sys.error(s"Could not create a TypeTag from type $t. This is a bug if $t is a column type.")
      )
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
