package com.google.cloud.spark.bigquery.integration;

import static com.google.common.truth.Truth.assertThat;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class QueryPushdownIntegrationTestBase extends SparkBigQueryIntegrationTestBase {

  @Test
  public void testStringFunctionExpressions() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);
    df =
        df.selectExpr(
                "word",
                "ASCII(word) as ascii",
                "LENGTH(word) as length",
                "LOWER(word) as lower",
                "LPAD(word, 10, '*') as lpad",
                "RPAD(word, 10, '*') as rpad",
                "TRANSLATE(word, 'a', '*') as translate",
                "TRIM(concat('    ', word, '    ')) as trim",
                "LTRIM(concat('    ', word, '    ')) as ltrim",
                "RTRIM(concat('    ', word, '    ')) as rtrim",
                "UPPER(word) as upper",
                "INSTR(word, 'a') as instr",
                "INITCAP(word) as initcap",
                "CONCAT(word, '*', '!!') as concat",
                "FORMAT_STRING('*%s*', word) as format_string",
                "FORMAT_NUMBER(10.2345, 1) as format_number",
                "REGEXP_EXTRACT(word, '([A-Za-z]+$)', 1) as regexp_extract",
                "REGEXP_REPLACE(word, '([A-Za-z]+$)', 'replacement') as regexp_replace",
                "SUBSTR(word, 2, 2) as substr",
                "SOUNDEX(word) as soundex")
            .where("word = 'augurs'");
    List<Row> result = df.collectAsList();
    Row r1 = result.get(0);
    assertThat(r1.get(0)).isEqualTo("augurs"); // word
    assertThat(r1.get(1)).isEqualTo(97); // ASCII(word)
    assertThat(r1.get(2)).isEqualTo(6); // LENGTH(word)
    assertThat(r1.get(3)).isEqualTo("augurs"); // LOWER(word)
    assertThat(r1.get(4)).isEqualTo("****augurs"); // LPAD(word, 10, '*')
    assertThat(r1.get(5)).isEqualTo("augurs****"); // LPAD(word, 10, '*')
    assertThat(r1.get(6)).isEqualTo("*ugurs"); // TRANSLATE(word, 'a', '*')
    assertThat(r1.get(7)).isEqualTo("augurs"); // TRIM(concat('    ', word, '    '))
    assertThat(r1.get(8)).isEqualTo("augurs    "); // LTRIM(concat('    ', word, '    '))
    assertThat(r1.get(9)).isEqualTo("    augurs"); // RTRIM(concat('    ', word, '    '))
    assertThat(r1.get(10)).isEqualTo("AUGURS"); // UPPER(word)
    assertThat(r1.get(11)).isEqualTo(1); // INSTR(word, 'a')
    assertThat(r1.get(12)).isEqualTo("Augurs"); // INITCAP(word)
    assertThat(r1.get(13)).isEqualTo("augurs*!!"); // CONCAT(word, '*', '!!')
    assertThat(r1.get(14)).isEqualTo("*augurs*"); // FORMAT_STRING('*%s*', word)
    assertThat(r1.get(15)).isEqualTo("10.2"); // FORMAT_NUMBER(10.2345, 1)
    assertThat(r1.get(16)).isEqualTo("augurs"); // REGEXP_EXTRACT(word, '([A-Za-z]+$)', 1)
    assertThat(r1.get(17))
        .isEqualTo("replacement"); // REGEXP_REPLACE(word, '([A-Za-z]+$)', 'replacement')
    assertThat(r1.get(18)).isEqualTo("ug"); // SUBSTR(word, 2, 2)
    assertThat(r1.get(19)).isEqualTo("a262"); // SOUNDEX(word)
  }

  @Test
  public void testDateFunctionExpressions() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);

    df.createOrReplaceTempView("shakespeare");

    List<Row> result =
        spark
            .sql(
                "SELECT "
                    + "DATE_ADD('2016-07-30', 1), "
                    + "DATE_SUB('2016-07-30', 5), "
                    + "cast('2017-07-30' AS date) + INTERVAL '30' day,"
                    + "MONTH('2016-07-30'), "
                    + "QUARTER('2016-07-30'), "
                    + "YEAR('2016-07-30'), "
                    + "TRUNC('2015-03-05', 'YEAR') "
                    + "FROM shakespeare "
                    + "WHERE word = 'augurs'")
            .collectAsList();

    Row r1 = result.get(0);
    assertThat(r1.get(0).toString()).isEqualTo("2016-07-31"); // DATE_ADD('2016-07-30', 1)
    assertThat(r1.get(1).toString()).isEqualTo("2016-07-25"); // DATE_SUB('2016-07-30', 5)
    assertThat(r1.get(2).toString()).isEqualTo("2017-08-29"); // cast('2017-07-30' AS date) + INTERVAL '30' day
    assertThat(r1.get(3).toString()).isEqualTo("7"); // MONTH('2016-07-30')
    assertThat(r1.get(4).toString()).isEqualTo("3"); // QUARTER('2016-07-30')
    assertThat(r1.get(5).toString()).isEqualTo("2016"); // YEAR('2016-07-30')
    assertThat(r1.get(6).toString()).isEqualTo("2015-01-01"); // TRUNC('2015-03-05', 'YEAR')
  }
}
