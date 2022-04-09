package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.{Alias, ExprId, Expression}
import org.apache.spark.sql.types.Metadata

trait ExpressionFactory {
  def createAlias(child: Expression, name: String, exprId: ExprId, qualifier: Seq[String], explicitMetadata: Option[Metadata]): Alias
}
