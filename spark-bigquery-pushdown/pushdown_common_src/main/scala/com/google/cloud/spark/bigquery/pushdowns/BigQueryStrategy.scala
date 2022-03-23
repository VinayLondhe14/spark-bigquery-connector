package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.SparkPlan

class BigQueryStrategy extends Strategy {
  /** This iterator automatically increments every time it is used,
   * and is for aliasing subqueries.
   */
  private final val alias = Iterator.from(0).map(n => s"SUBQUERY_$n")

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val cleanedPlan = cleanUpLogicalPlan(plan)

    val bigQuerySqlStatement = queryBuilder.createBigQuerySqlStatement(cleanedPlan)
    val rdd = queryBuilder.getRelation.buildRDDFromSql(bigQuerySqlStatement)
    val output = queryBuilder.getOutput


    BigQueryPlan(output, rdd)
  }

  def cleanUpLogicalPlan(plan: LogicalPlan): LogicalPlan = {
    plan.transform({
      case Project(Nil, child) => child
      case SubqueryAlias(_, child) => child
    })
  }
}
