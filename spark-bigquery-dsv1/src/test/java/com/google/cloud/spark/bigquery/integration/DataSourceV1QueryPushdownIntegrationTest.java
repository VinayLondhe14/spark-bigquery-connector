package com.google.cloud.spark.bigquery.integration;

import com.google.cloud.spark.bigquery.BigQueryConnectorUtils;
import org.junit.Test;

public class DataSourceV1QueryPushdownIntegrationTest extends QueryPushdownIntegrationTestBase {

  public DataSourceV1QueryPushdownIntegrationTest() {
    BigQueryConnectorUtils.enablePushdownSession(spark);
  }

  @Test
  public void testOrder() {
    testOrderBy();
  }

  @Test
  public void testAgg() {
    testAggregation();
  }

  @Test
  public void testTpc() {
    testTpcDsQ61();
  }
}
