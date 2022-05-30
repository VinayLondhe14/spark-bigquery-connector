package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.{blockStatement, makeStatement}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}

/** The query for union.
 *
 * @constructor
 * @param children Children of the union expression.
 */
case class UnionQuery(
    expressionConverter: SparkExpressionConverter,
    expressionFactory: SparkExpressionFactory,
    children: Seq[BigQuerySQLQuery],
    alias: String,
    outputCols: Option[Seq[Attribute]] = None)
  extends BigQuerySQLQuery(expressionConverter, expressionFactory, alias, children,
    outputAttributes = Some(children.head.output),
    visibleAttributeOverride =
      Some(children.foldLeft(Seq.empty[Attribute])((x, y) => x ++ y.output).map(
        a =>
          AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
            a.exprId,
            Seq[String](alias)
          )))) {

  override def getStatement(useAlias: Boolean): BigQuerySQLStatement = {
    val query =
      if (children.nonEmpty) {
        makeStatement(
          children.map(c => blockStatement(c.getStatement())),
          "UNION ALL"
        )
      } else {
        EmptyBigQuerySQLStatement()
      }

    if (useAlias) {
      blockStatement(query, alias)
    } else {
      query
    }
  }

  override def find[T](query: PartialFunction[BigQuerySQLQuery, T]): Option[T] =
    query
      .lift(this)
      .orElse(
        children
          .map(q => q.find(query))
          .view
          .foldLeft[Option[T]](None)(_ orElse _)
      )
}
