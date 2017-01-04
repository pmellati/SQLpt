package sqlpt

import java.util.UUID.randomUUID

import sqlpt.column._
import Column._
import sqlpt.ast.expressions.{Selection, Table}, Table.Partitioning.Unpartitioned
import sqlpt.ast.statements.Statements._
import sqlpt.ast.statements.StringStatement
import sqlpt.ast.statements.Insertion

import scala.annotation.StaticAnnotation
import scala.reflect.runtime.universe.TypeTag

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
    type Columns      <: Product
    type Partitioning <: Table.Partitioning

    def name:         String
    def partitioning: Partitioning  // TODO: Should become a private def, like 'cols'.

    final def table(implicit ctt: TypeTag[Columns]) = Table(name, cols, partitioning)

    protected type Named = TableDefAnnotations.Named

    /** TODO: We may be able to check at compile-time that 'Columns' is a case class. See:
      *   http://stackoverflow.com/questions/30233178/how-to-check-if-some-t-is-a-case-class-at-compile-time-in-scala
      */
    private def cols(implicit ctt: TypeTag[Columns]): Columns = {
      import reflect.runtime.universe._

      val typ = typeOf[Columns]

      // TODO: Replace with a compile-time check.
      if (!typ.typeSymbol.isClass || !typ.typeSymbol.asClass.isCaseClass)
        throw new RuntimeException(s"${typ.typeSymbol.fullName} is not a case class.")

      /** TODO: Is there a better way to do this?
        * This approach is error prone, as one has to remember to keep this function in sync with the set of possible
        * column types.
        */
      def columnTypeToTypeTag(t: reflect.runtime.universe.Type): TypeTag[_ <: Column.Type] =
        if      (t <:< typeOf[Type.Num]) implicitly[TypeTag[Type.Num]]
        else if (t <:< typeOf[Type.Str]) implicitly[TypeTag[Type.Str]]
        else sys.error("Bug!!")

      val ctor = typ.typeSymbol.asClass.typeSignature.members.filter(_.isConstructor).head.asMethod

      val params = ctor.paramLists.head

      val columns = params.map {param =>
        val columnName = param.annotations.find {
          _.tree.tpe <:< typeOf[Named]}
        .map {
          _.tree.children.tail.head.toString   /** See: [[scala.reflect.api.Annotations]] */
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

  object TableDefAnnotations {
    class Named(val columnName: String) extends StaticAnnotation
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
