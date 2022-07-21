package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}

case class WindowQuery(expressionConverter: SparkExpressionConverter,
                       expressionFactory: SparkExpressionFactory,
                       windowExpressions: Seq[NamedExpression],
                       child: BigQuerySQLQuery,
                       alias: String,
                       fields: Option[Seq[Attribute]])
  extends BigQuerySQLQuery(
    expressionConverter = expressionConverter,
    expressionFactory = expressionFactory,
    alias = alias,
    children = Seq(child),
    projections = WindowQuery.getWindowProjections(windowExpressions, child, fields),
    outputAttributes = None) {
}

object WindowQuery {
  def getWindowProjections(windowExpressions: Seq[NamedExpression], child: BigQuerySQLQuery, fields: Option[Seq[Attribute]]): Option[Seq[NamedExpression]] = {
    val projectionVector = windowExpressions ++ child.outputWithQualifier

    // We need to reorder the projections based on the output vector
    val orderedProjections =
      fields.map(_.map(reference => {
        val origPos = projectionVector.map(_.exprId).indexOf(reference.exprId)
        projectionVector(origPos)
      }))

    orderedProjections
  }
}