package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.SparkSession

class Spark24BigQueryPushdown extends SparkBigQueryPushdown {
  override def enable(session: SparkSession, bigQueryStrategy: BigQueryStrategy): Unit = {
    SparkBigQueryPushdownUtil.enableBigQueryStrategy(session, bigQueryStrategy)
  }

  override def disable(session: SparkSession): Unit = {
    SparkBigQueryPushdownUtil.disableBigQueryStrategy(session)
  }

  override def supportsSparkVersion(sparkVersion: String): Boolean = {
    sparkVersion.startsWith("2.4")
  }

  override def getBigQueryStrategy(expressionConverter: ExpressionConverter): BigQueryStrategy = {
    new Spark24BigQueryStrategy(expressionConverter)
  }

  override def getExpressionConverter: ExpressionConverter = {
    new Spark24ExpressionConverter
  }
}
