package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class QueryBuilder {
  /** This iterator automatically increments every time it is used,
   * and is for aliasing subqueries.
   */
  private final val alias = Iterator.from(0).map(n => s"SUBQUERY_$n")
  private var relation: Option[DirectBigQueryRelation] = None

  def getRelation: Option[DirectBigQueryRelation] = relation

  def getBigQuerySqlStatement(plan: LogicalPlan): BigQuerySqlStatement = {

  }
}
