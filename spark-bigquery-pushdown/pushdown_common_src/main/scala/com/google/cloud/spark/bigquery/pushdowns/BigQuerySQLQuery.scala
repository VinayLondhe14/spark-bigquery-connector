package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.{blockStatement, mkStatement}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Expression, NamedExpression}

abstract class BigQuerySQLQuery(
   expressionConverter: ExpressionConverter,
   alias: String,
   children: Seq[BigQuerySQLQuery] = Seq.empty,
   projections: Option[Seq[NamedExpression]] = None,
   outputAttributes: Option[Seq[Attribute]] = None
 ) {

  val conjunctionStatement: BigQuerySQLStatement = EmptyBigQuerySQLStatement()

  val sourceStatement: BigQuerySQLStatement =
    if (children.nonEmpty) {
      mkStatement(children.map(_.getStatement(true)), conjunctionStatement)
    } else {
      conjunctionStatement
    }

  /** What comes after the FROM clause. */
  val suffixStatement: BigQuerySQLStatement = EmptyBigQuerySQLStatement()

  val colSet: Seq[Attribute] =
    children.foldLeft(Seq.empty[Attribute])(
        (x, y) => {
          val attrs = y.outputWithQualifier
          x ++ attrs
        }
      )

  val processedProjections: Option[Seq[NamedExpression]] = projections
    .map(
      p =>
        p.map(
          e =>
            colSet.find(c => c.exprId == e.exprId) match {
              case Some(a) if e.isInstanceOf[AttributeReference] =>
                AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
                  a.exprId
                )
              case _ => e
            }
        )
    )
    .map(p => renameColumns(p, alias))

  val output: Seq[Attribute] = {
    outputAttributes.getOrElse(
      processedProjections.map(p => p.map(_.toAttribute)).getOrElse {
        if (children.isEmpty) {
          throw new BigQueryPushdownException(
            "Query output attributes must not be empty when it has no children."
          )
        } else {
          children
            .foldLeft(Seq.empty[Attribute])((x, y) => x ++ y.output)
        }
      }
    )
  }

  val outputWithQualifier: Seq[AttributeReference] = output.map(
    a =>
      AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
        a.exprId,
        Seq[String](alias)
      )
  )

  val columns: Option[BigQuerySQLStatement] =
    processedProjections.map(
      p => mkStatement(p.map(expressionConverter.convertStatement(_, colSet)), ",")
    )

  /** Converts this query into a String representing the SQL.
   *
   * @param useAlias Whether or not to alias this translated block of SQL.
   * @return SQL statement for this query.
   */
  def getStatement(useAlias: Boolean = false): BigQuerySQLStatement = {
    val stmt =
      ConstantString("SELECT") + columns.getOrElse(ConstantString("*") !) + "FROM" +
        sourceStatement + suffixStatement

    if (useAlias) {
      blockStatement(stmt, alias)
    } else {
      stmt
    }
  }

  def expressionToStatement(expr: Expression): BigQuerySQLStatement =
    expressionConverter.convertStatement(expr, colSet)
}

/** The query for a base type (representing a table or view).
 *
 * @constructor
 * @param tableName   The BigQuery table to be queried
 * @param refColumns Columns used to override the output generation for the QueryHelper.
 *                   These are the columns resolved by SnowflakeRelation.
 * @param alias      Query alias.
 */
case class SourceQuery(expressionConverter: ExpressionConverter,
                       tableName: String,
                       refColumns: Seq[Attribute],
                       alias: String)
  extends BigQuerySQLQuery(expressionConverter, alias, outputAttributes = Some(refColumns)) {

  override val conjunctionStatement: BigQuerySQLStatement = blockStatement(ConstantString("`" + tableName + "`").toStatement, "bq_connector_query_alias", isSourceQuery = true)
}

/** The query for a filter operation.
 *
 * @constructor
 * @param conditions The filter condition.
 * @param child      The child node.
 * @param alias      Query alias.
 */
case class FilterQuery(
   expressionConverter: ExpressionConverter,
   conditions: Seq[Expression],
   child: BigQuerySQLQuery,
   alias: String
) extends BigQuerySQLQuery(expressionConverter, alias, children = Seq(child)) {

  override val suffixStatement: BigQuerySQLStatement =
    ConstantString("WHERE") + mkStatement(
      conditions.map(expressionToStatement),
      "AND"
    )
}

/** The query for a projection operation.
 *
 * @constructor
 * @param projectionColumns The projection columns.
 * @param child   The child node.
 * @param alias   Query alias.
 */
case class ProjectQuery(
  expressionConverter: ExpressionConverter,
  projectionColumns: Seq[NamedExpression],
  child: BigQuerySQLQuery,
  alias: String
) extends BigQuerySQLQuery(expressionConverter, alias, children = Seq(child), projections = Some(projectionColumns)) {}

/** The query for a aggregation operation.
 *
 * @constructor
 * @param projectionColumns The projection columns, containing also the aggregate expressions.
 * @param groups  The grouping columns.
 * @param child   The child node.
 * @param alias   Query alias.
 */
case class AggregateQuery(
  expressionConverter: ExpressionConverter,
  projectionColumns: Seq[NamedExpression],
  groups: Seq[Expression],
  child: BigQuerySQLQuery,
  alias: String
) extends BigQuerySQLQuery(expressionConverter, alias, children = Seq(child), projections = if (projectionColumns.isEmpty) None else Some(projectionColumns)) {

  override val suffixStatement: BigQuerySQLStatement =
    if (groups.nonEmpty) {
      ConstantString("GROUP BY") +
        mkStatement(groups.map(expressionToStatement), ",")
    } else {
      ConstantString("LIMIT 1").toStatement
    }
}
