package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Union}

// The Spark APIs for Union are not compatible between Spark 2.4 and 3.1.
// So, we create this extractor that only extracts those parameters that are
// used for creating the union query
object UnionExtractor {
  def unapply(node: LogicalPlan): Option[(Seq[LogicalPlan])] =
    node match {
      case _: Union =>
        Some(node.children)

      // We should never reach here
      case _ => None
    }
}
