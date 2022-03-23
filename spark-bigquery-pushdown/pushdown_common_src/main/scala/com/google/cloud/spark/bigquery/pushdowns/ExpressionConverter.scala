package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.{blockStatement, mkStatement}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

class ExpressionConverter {
  def convertStatement(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
    convertAggregateExpressions(expression, fields)
      .orElse(convertBasicExpressions(expression, fields))
      .getOrElse(throw new UnsupportedOperationException("Pushdown unsupported"))
  }

  private[querygeneration] final def convertStatements(fields: Seq[Attribute], expressions: Expression*): BigQuerySQLStatement =
    mkStatement(expressions.map(convertStatement(_, fields)), ",")


  def convertAggregateExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    expression match {
      case _: AggregateExpression =>
        // Take only the first child, as all of the functions below have only one.
        expression.children.headOption.flatMap(agg_fun => {
          Option(agg_fun match {
            case _: Average | _: Corr | _: CovPopulation | _: CovSample | _: Count |
                 _: Max | _: Min | _: Sum | _: StddevPop | _: StddevSamp |
                 _: VariancePop | _: VarianceSamp =>
              val distinct: BigQuerySQLStatement =
                if (expression.sql contains "(DISTINCT ") ConstantString("DISTINCT") !
                else EmptyBigQuerySQLStatement()

              ConstantString(agg_fun.prettyName.toUpperCase) +
                blockStatement(
                  distinct + convertStatements(fields, agg_fun.children: _*)
                )
          })
        })
      case _ => None
    }
  }

  def convertBasicExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case a: Attribute => addAttributeStatement(a, fields)
      case And(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "AND" + convertStatement(
            right,
            fields
          )
        )
      case Or(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "OR" + convertStatement(
            right,
            fields
          )
        )
      case b: BinaryOperator =>
        blockStatement(
          convertStatement(b.left, fields) + b.symbol + convertStatement(
            b.right,
            fields
          )
        )
      case l: Literal =>
        l.dataType match {
          case StringType =>
            if (l.value == null) {
              ConstantString("NULL") !
            } else {
              StringVariable(Some(l.toString())) ! // else "'" + str + "'"
            }
          case _ =>
            l.value match {
              case v: Int => IntVariable(Some(v)) !
              case v: Long => LongVariable(Some(v)) !
              case v: Short => ShortVariable(Some(v)) !
              case v: Boolean => BooleanVariable(Some(v)) !
              case v: Float => FloatVariable(Some(v)) !
              case v: Double => DoubleVariable(Some(v)) !
              case v: Byte => ByteVariable(Some(v)) !
              case _ => ConstantStringVal(l.value) !
            }
        }

      case _ => null
    })
  }

  def convertBooleanExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case In(child, list) if list.forall(_.isInstanceOf[Literal]) =>
        convertStatement(child, fields) + "IN" +
          blockStatement(convertStatements(fields, list: _*))
      case IsNull(child) =>
        blockStatement(convertStatement(child, fields) + "IS NULL")
      case IsNotNull(child) =>
        blockStatement(convertStatement(child, fields) + "IS NOT NULL")
      case Not(child) => {
        child match {
          case EqualTo(left, right) =>
            blockStatement(
              convertStatement(left, fields) + "!=" +
                convertStatement(right, fields)
            )
          // NOT ( GreaterThanOrEqual, LessThanOrEqual,
          // GreaterThan and LessThan ) have been optimized by spark
          // and are handled by BinaryOperator in BasicStatement.
          case GreaterThanOrEqual(left, right) =>
            convertStatement(LessThan(left, right), fields)
          case LessThanOrEqual(left, right) =>
            convertStatement(GreaterThan(left, right), fields)
          case GreaterThan(left, right) =>
            convertStatement(LessThanOrEqual(left, right), fields)
          case LessThan(left, right) =>
            convertStatement(GreaterThanOrEqual(left, right), fields)
          case _ =>
            ConstantString("NOT") +
              blockStatement(convertStatement(child, fields))
        }
      }
      case Contains(child, Literal(pattern: UTF8String, StringType)) =>
        convertStatement(child, fields) + "LIKE" + s"'%${pattern.toString}%'"
      case EndsWith(child, Literal(pattern: UTF8String, StringType)) =>
        convertStatement(child, fields) + "LIKE" + s"'%${pattern.toString}'"
      case StartsWith(child, Literal(pattern: UTF8String, StringType)) =>
        convertStatement(child, fields) + "LIKE" + s"'${pattern.toString}%'"

      case _ => null
    })
  }
}
