package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.bigquery.connector.common.BigQueryConnectorException
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, GlobalLimit, Limit, LocalLimit, LogicalPlan, Project, ReturnAnswer, Sort, SubqueryAlias, UnaryNode, Window}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

class BigQueryStrategy(expressionConverter: ExpressionConverter) extends Strategy with Logging {
  /** This iterator automatically increments every time it is used,
   * and is for aliasing subqueries.
   */
  private final val alias = Iterator.from(0).map(n => s"SUBQUERY_$n")

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val cleanedPlan = cleanUpLogicalPlan(plan)

    val treeRoot = generateTree(cleanedPlan)
    if (treeRoot == null) {
      return Nil
    }

    val sparkPlan = generateSparkPlan(treeRoot)
    if (sparkPlan == null) {
      return Nil
    }

    Seq(sparkPlan)
  }

  def cleanUpLogicalPlan(plan: LogicalPlan): LogicalPlan = {
    plan.transform({
      case Project(Nil, child) => child
      case SubqueryAlias(_, child) => child
    })
  }

  private def generateTree(plan: LogicalPlan): BigQuerySQLQuery = {
    try {
      generateQueries(plan).get
    } catch {
      case e: Exception =>
        logWarning("Query pushdown failed: ", e)
        null
    }
  }

  private def generateQueries(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    plan match {
      case l@LogicalRelation(bqRelation: DirectBigQueryRelation, _, _, _) =>
        Some(SourceQuery(expressionConverter, bqRelation.tableName, l.output, alias.next))

      case UnaryOp(child) =>
        generateQueries(child) map { subQuery =>
          plan match {
            case Filter(condition, _) =>
              FilterQuery(expressionConverter, Seq(condition), subQuery, alias.next)

            case Project(fields, _) =>
              ProjectQuery(expressionConverter, fields, subQuery, alias.next)

            case Aggregate(groups, fields, _) =>
              AggregateQuery(expressionConverter, fields, groups, subQuery, alias.next)

            case Limit(limitExpr, Sort(orderExpr, true, _)) =>
              SortLimitQuery(expressionConverter, Some(limitExpr), orderExpr, subQuery, alias.next)

            case Limit(limitExpr, _) =>
              SortLimitQuery(expressionConverter, Some(limitExpr), Seq.empty, subQuery, alias.next)

            case Sort(orderExpr, true, Limit(limitExpr, _)) =>
              SortLimitQuery(expressionConverter, Some(limitExpr), orderExpr, subQuery, alias.next)

            case Sort(orderExpr, true, _) =>
              SortLimitQuery(expressionConverter, None, orderExpr, subQuery, alias.next)

            case _ => subQuery
          }
        }

      case _ =>
        throw new BigQueryConnectorException.PushdownUnsupportedException(
          s"Query pushdown failed in generateQueries for node ${plan.nodeName} in ${plan.getClass.getName}"
        )
    }
  }

  private def generateSparkPlan(treeRoot: BigQuerySQLQuery): SparkPlan = {
    try {
      val bigQuerySqlStatement = treeRoot.getStatement()
//      val rdd = queryBuilder.getRelation.buildRDDFromSql(bigQuerySqlStatement)
//
//      val output = treeRoot.getOutput
//      BigQueryPlan(output, rdd)
      null
    } catch {
      case e: Exception =>
        logWarning("Query pushdown failed: ", e)
        null
    }
  }
}

/** Extractor for supported unary operations. */
object UnaryOp {

  def unapply(node: UnaryNode): Option[LogicalPlan] =
    node match {
      case _: Filter | _: Project | _: GlobalLimit | _: LocalLimit |
           _: Aggregate | _: Sort | _: ReturnAnswer | _: Window =>
        Some(node.child)

      case _ => None
    }
}
