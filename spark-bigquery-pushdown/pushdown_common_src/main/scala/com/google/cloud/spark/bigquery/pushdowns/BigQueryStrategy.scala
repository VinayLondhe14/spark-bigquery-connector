package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.SparkPlan

class BigQueryStrategy(queryBuilder: QueryBuilder) extends Strategy {
  def cleanUpLogicalPlan(plan: LogicalPlan): LogicalPlan = {
    plan.transform({
      case Project(Nil, child) => child
      case SubqueryAlias(_, child) => child
    })
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val cleanedPlan = cleanUpLogicalPlan(plan)

    val bigQuerySqlStatement = queryBuilder.createBigQuerySqlStatement(cleanedPlan)
    val rdd = queryBuilder.getRelation.buildRDDFromSql(bigQuerySqlStatement)
    val output = queryBuilder.getOutput


    BigQueryPlan(output, rdd)
  }
}
