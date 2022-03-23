package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.SparkSession

object SparkBigQueryPushdownUtil {
  def enableBigQueryStrategy(session: SparkSession, bigQueryStrategy: BigQueryStrategy): Unit = {
    if (!session.experimental.extraStrategies.exists(
      s => s.isInstanceOf[BigQueryStrategy]
    )) {
      session.experimental.extraStrategies ++= Seq(bigQueryStrategy)
    }
  }

  def disableBigQueryStrategy(session: SparkSession): Unit = {
    session.experimental.extraStrategies = session.experimental.extraStrategies
      .filterNot(strategy => strategy.isInstanceOf[BigQueryStrategy])
  }

  def blockStatement(stmt: BigQuerySQLStatement): BigQuerySQLStatement =
    ConstantString("(") + stmt + ")"

  def blockStatement(stmt: BigQuerySQLStatement, alias: String, isSourceQuery: Boolean = false): BigQuerySQLStatement =
    if (isSourceQuery) stmt + "AS" + ConstantString(alias.toUpperCase).toStatement
    else blockStatement(stmt) + "AS" + ConstantString(alias.toUpperCase).toStatement

  def mkStatement(seq: Seq[BigQuerySQLStatement], delimiter: String): BigQuerySQLStatement =
    mkStatement(seq, ConstantString(delimiter) !)

  def mkStatement(seq: Seq[BigQuerySQLStatement], delimiter: BigQuerySQLStatement): BigQuerySQLStatement =
    seq.foldLeft(EmptyBigQuerySQLStatement()) {
      case (left, stmt) =>
        if (left.isEmpty) stmt else left + delimiter + stmt
    }
}
