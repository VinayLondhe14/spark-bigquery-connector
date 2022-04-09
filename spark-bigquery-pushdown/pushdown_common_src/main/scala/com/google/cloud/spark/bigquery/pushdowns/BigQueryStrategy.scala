package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.bigquery.connector.common.BigQueryConnectorException
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, GlobalLimit, Limit, LocalLimit, LogicalPlan, Project, ReturnAnswer, Sort, SubqueryAlias, UnaryNode, Window}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

class BigQueryStrategy(expressionConverter: ExpressionConverter, expressionFactory: ExpressionFactory) extends Strategy with Logging {
  /** This iterator automatically increments every time it is used,
   * and is for aliasing subqueries.
   */
  private final val alias = Iterator.from(0).map(n => s"SUBQUERY_$n")
  protected var directBigQueryRelation: Option[DirectBigQueryRelation] = None

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
        logInfo("Query pushdown failed: ", e)
        null
    }
  }

  private def generateQueries(plan: LogicalPlan): Option[BigQuerySQLQuery] = {
    plan match {
      case l@LogicalRelation(bqRelation: DirectBigQueryRelation, _, _, _) =>
        directBigQueryRelation = Some(bqRelation)
        Some(SourceQuery(expressionConverter, expressionFactory, bqRelation.tableName, l.output, alias.next))

      case UnaryOp(child) =>
        generateQueries(child) map { subQuery =>
          plan match {
            case Filter(condition, _) =>
              FilterQuery(expressionConverter, expressionFactory, Seq(condition), subQuery, alias.next)

            case Project(fields, _) =>
              ProjectQuery(expressionConverter, expressionFactory, fields, subQuery, alias.next)

            case Aggregate(groups, fields, _) =>
              AggregateQuery(expressionConverter, expressionFactory, fields, groups, subQuery, alias.next)

            case Limit(limitExpr, Sort(orderExpr, true, _)) =>
              SortLimitQuery(expressionConverter, expressionFactory, Some(limitExpr), orderExpr, subQuery, alias.next)

            case Limit(limitExpr, _) =>
              SortLimitQuery(expressionConverter, expressionFactory, Some(limitExpr), Seq.empty, subQuery, alias.next)

            case Sort(orderExpr, true, Limit(limitExpr, _)) =>
              SortLimitQuery(expressionConverter, expressionFactory, Some(limitExpr), orderExpr, subQuery, alias.next)

            case Sort(orderExpr, true, _) =>
              SortLimitQuery(expressionConverter, expressionFactory, None, orderExpr, subQuery, alias.next)

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
      BigQueryPlan(treeRoot.getOutput, getRdd(treeRoot))
    } catch {
      case e: Exception =>
        logInfo("Query pushdown failed: ", e)
        null
    }
  }

  private def getRdd(treeRoot: BigQuerySQLQuery): RDD[InternalRow] = {
    if (directBigQueryRelation == null) {
      throw new BigQueryConnectorException.PushdownException(
        "Query output attributes must not be empty when it has no children."
      )
    }

    directBigQueryRelation.get.buildScanFromSQL(treeRoot.getStatement().toString)
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
