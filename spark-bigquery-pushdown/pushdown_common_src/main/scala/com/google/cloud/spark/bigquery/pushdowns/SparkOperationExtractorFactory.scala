package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, BinaryNode, Filter, GlobalLimit, Join, LocalLimit, LogicalPlan, Project, ReturnAnswer, Sort, UnaryNode, Union, Window}

class SparkOperationExtractorFactory {
  def extractBinaryOperation(node: LogicalPlan): Option[(LogicalPlan, LogicalPlan)] =
    Option(node match {
      case _: Join => (node.asInstanceOf[BinaryNode].left, node.asInstanceOf[BinaryNode].right)
      case _ => null
    })

  def extractCastOperation(node: Expression): Option[Expression] =
    node match {
      case _: Cast =>
        Some(node)

      case _ => None
    }

  def extractUnaryOperation(node: LogicalPlan): Option[LogicalPlan] =
    node match {
      case _: Filter | _: Project | _: GlobalLimit | _: LocalLimit |
           _: Aggregate | _: Sort | _: ReturnAnswer | _: Window =>
        Some(node.asInstanceOf[UnaryNode].child)

      case _ => None
    }

  def extractUnionOperation(node: Union): Option[Seq[LogicalPlan]] =
    Option(node match {
      case _: Union => node.children
      case _ => null
    })
}
