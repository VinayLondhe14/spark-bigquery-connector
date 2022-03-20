package com.google.cloud.spark.bigquery.pushdowns

class BigQuerySQLStatement(val list: List[StatementElement] = Nil) {
  def +(element: StatementElement): BigQuerySQLStatement =
    new BigQuerySQLStatement(element :: list)

  def +(statement: BigQuerySQLStatement): BigQuerySQLStatement =
    new BigQuerySQLStatement(statement.list ::: list)

  def +(str: String): BigQuerySQLStatement = this + ConstantString(str)

  def isEmpty: Boolean = list.isEmpty

  def statementString: String = {
    val buffer = new StringBuilder
    val sql = list.reverse

    sql.foreach {
      case x: ConstantString =>
        if (buffer.nonEmpty && buffer.last != ' ') {
          buffer.append(" ")
        }
        buffer.append(x)
      case x: VariableElement[_] =>
        if (buffer.nonEmpty && buffer.last != ' ') {
          buffer.append(" ")
        }
        buffer.append(x.value)
        buffer.append("(")
        buffer.append(x.variable)
        buffer.append(")")
    }

    buffer.toString()
  }

}

object EmptySnowflakeSQLStatement {
  def apply(): BigQuerySQLStatement = new BigQuerySQLStatement()
}

sealed trait StatementElement {

  val value: String

  val isVariable: Int = 0

  def +(element: StatementElement): BigQuerySQLStatement =
    new BigQuerySQLStatement(element :: List[StatementElement](this))

  def +(statement: BigQuerySQLStatement): BigQuerySQLStatement =
    new BigQuerySQLStatement(
      statement.list ::: List[StatementElement](this)
    )

  def +(str: String): BigQuerySQLStatement = this + ConstantString(str)

  override def toString: String = value

  def ! : BigQuerySQLStatement = toStatement

  def toStatement: BigQuerySQLStatement =
    new BigQuerySQLStatement(List[StatementElement](this))

  def sql: String = value
}

case class ConstantString(override val value: String) extends StatementElement

sealed trait VariableElement[T] extends StatementElement {
  override val value = "?"

  override val isVariable: Int = 1

  val variable: Option[T]

  override def sql: String = if (variable.isDefined) variable.get.toString else "NULL"

}

case class StringVariable(override val variable: Option[String])
  extends VariableElement[String] {
  override def sql: String = if (variable.isDefined) s"""'${variable.get}'""" else "NULL"
}

case class IntVariable(override val variable: Option[Int])
  extends VariableElement[Int]

case class LongVariable(override val variable: Option[Long])
  extends VariableElement[Long]

case class ShortVariable(override val variable: Option[Short])
  extends VariableElement[Short]

case class FloatVariable(override val variable: Option[Float])
  extends VariableElement[Float]

case class DoubleVariable(override val variable: Option[Double])
  extends VariableElement[Double]

case class BooleanVariable(override val variable: Option[Boolean])
  extends VariableElement[Boolean]

case class ByteVariable(override val variable: Option[Byte])
  extends VariableElement[Byte]
