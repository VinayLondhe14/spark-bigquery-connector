package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.types.{LongType, StringType}

object TestConstants {
   val TABLE_NAME = "test_project:test_dataset.test_table"
   val SOURCE_QUERY_ALIAS = "SUBQUERY_0"
   val FILTER_QUERY_ALIAS = "SUBQUERY_1"
   val PROJECT_QUERY_ALIAS = "SUBQUERY_2"

   val expressionConverter: SparkExpressionConverter = new SparkExpressionConverter {}
   val schoolIdAttributeReference: AttributeReference = AttributeReference.apply("SchoolID", LongType)(ExprId.apply(1))
   val schoolNameAttributeReference: AttributeReference = AttributeReference.apply("SchoolName", StringType)(ExprId.apply(2))
}
