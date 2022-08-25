package com.google.cloud.spark.bigquery.integration;

import static com.google.common.truth.Truth.assertThat;
import static java.time.temporal.TemporalAdjusters.firstDayOfYear;

import com.google.cloud.spark.bigquery.SparkBigQueryConfig.WriteMethod;
import com.google.cloud.spark.bigquery.integration.model.NumStruct;
import com.google.cloud.spark.bigquery.integration.model.StringStruct;
import com.google.common.collect.ImmutableList;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.IsoFields;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
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
    // This table only has one row and one column which is today's date
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load("bigquery-public-data.google_political_ads.last_updated");

    df.createOrReplaceTempView("last_updated");

    List<Row> result =
        spark
            .sql(
                "SELECT "
                    + "report_data_updated_time, "
                    + "DATE_ADD(report_data_updated_time, 1), "
                    + "DATE_SUB(report_data_updated_time, 5), "
                    + "MONTH(report_data_updated_time), "
                    + "QUARTER(report_data_updated_time), "
                    + "YEAR(report_data_updated_time), "
                    + "TRUNC(report_data_updated_time, 'YEAR') "
                    + "FROM last_updated")
            .collectAsList();

    Row r1 = result.get(0);

    // Parsing the date rather than setting date to LocalDate.now() because the test will fail
    // in the edge case that the BigQuery read happens on an earlier date
    LocalDate date = LocalDateTime.parse(r1.get(0).toString()).toLocalDate();

    assertThat(r1.get(1).toString()).isEqualTo(date.plusDays(1L).toString()); // DATE_ADD
    assertThat(r1.get(2).toString()).isEqualTo(date.minusDays(5L).toString()); // DATE_SUB
    assertThat(r1.get(3)).isEqualTo(date.getMonth().getValue()); // MONTH
    assertThat(r1.get(4)).isEqualTo(date.get(IsoFields.QUARTER_OF_YEAR)); // QUARTER
    assertThat(r1.get(5)).isEqualTo(date.getYear()); // YEAR
    assertThat(r1.get(6).toString()).isEqualTo(date.with(firstDayOfYear()).toString()); // TRUNC
  }

  @Test
  public void testBasicExpressions() {
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
                    + "word_count & corpus_date, "
                    + "word_count | corpus_date, "
                    + "word_count ^ corpus_date, "
                    + "~ word_count, "
                    + "word <=> corpus "
                    + "FROM shakespeare "
                    + "WHERE word = 'augurs' AND corpus = 'sonnets'")
            .collectAsList();

    // Note that for this row, word_count equals 1 and corpus_date equals 0
    Row r1 = result.get(0);
    assertThat(r1.get(0).toString()).isEqualTo("0"); // 1 & 0
    assertThat(r1.get(1).toString()).isEqualTo("1"); // 1 | 0
    assertThat(r1.get(2).toString()).isEqualTo("1"); // 1 ^ 0
    assertThat(r1.get(3).toString()).isEqualTo("-2"); // ~1
    assertThat(r1.get(4)).isEqualTo(false); // 'augurs' <=> 'sonnets'
  }

  @Test
  public void testMathematicalFunctionExpressions() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);
    df =
        df.selectExpr(
                "word",
                "word_count",
                "ABS(-22) as Abs",
                "ACOS(1) as Acos",
                "ASIN(0) as Asin",
                "ROUND(ATAN(0.5),2) as Atan",
                "COS(0) as Cos",
                "COSH(0) as Cosh",
                "ROUND(EXP(1),2) as Exp",
                "FLOOR(EXP(1)) as Floor",
                "GREATEST(1,5,3,4) as Greatest",
                "LEAST(1,5,3,4) as Least",
                "ROUND(LOG(word_count, 2.71), 2) as Log",
                "ROUND(LOG10(word_count), 2) as Log10",
                "POW(word_count, 2) as Pow",
                "ROUND(RAND(10),2) as Rand",
                "SIN(0) as Sin",
                "SINH(0) as Sinh",
                "ROUND(SQRT(word_count), 2) as sqrt",
                "TAN(0) as Tan",
                "TANH(0) as Tanh",
                "ISNAN(word_count) as IsNan",
                "SIGNUM(word_count) as Signum")
            .where("word_count = 10 and word = 'glass'");
    List<Row> result = df.collectAsList();
    Row r1 = result.get(0);
    assertThat(r1.get(0)).isEqualTo("glass"); // word
    assertThat(r1.get(1)).isEqualTo(10); // word_count
    assertThat(r1.get(2)).isEqualTo(22); // ABS(-22)
    assertThat(r1.get(3)).isEqualTo(0.0); // ACOS(1)
    assertThat(r1.get(4)).isEqualTo(0.0); // ASIN(0)
    assertThat(r1.get(5)).isEqualTo(0.46); // ROUND(ATAN(0.5),2)
    assertThat(r1.get(6)).isEqualTo(1.0); // COS(0)
    assertThat(r1.get(7)).isEqualTo(1.0); // COSH(0)
    assertThat(r1.get(8)).isEqualTo(2.72); // ROUND(EXP(1),2)
    assertThat(r1.get(9)).isEqualTo(2); // FLOOR(EXP(1))
    assertThat(r1.get(10)).isEqualTo(5); // GREATEST(1,5,3,4)
    assertThat(r1.get(11)).isEqualTo(1); // LEAST(1,5,3,4)
    assertThat(r1.get(12)).isEqualTo(2.31); // ROUND(LOG(word_count, 2.71), 2)
    assertThat(r1.get(13)).isEqualTo(1.0); // ROUND(LOG10(word_count), 2)
    assertThat(r1.get(14)).isEqualTo(100.0); // POW(word_count, 2)
    assertThat(r1.get(16)).isEqualTo(0.0); // SIN(0)
    assertThat(r1.get(17)).isEqualTo(0.0); // SINH(0)
    assertThat(r1.get(18)).isEqualTo(3.16); // ROUND(SQRT(word_count), 2)
    assertThat(r1.get(19)).isEqualTo(0.0); // TAN(0)
    assertThat(r1.get(20)).isEqualTo(0.0); // TANH(0)
    assertThat(r1.get(21)).isEqualTo(false); // ISNAN(word_count)
    assertThat(r1.get(22)).isEqualTo(1.0); // SIGNUM(word_count)
  }

  @Test
  public void testMiscellaneousExpressions() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);
    df.createOrReplaceTempView("shakespeare");
    df =
        df.selectExpr(
                "word",
                "word_count AS WordCount",
                "CAST(word_count as string) AS cast",
                "SHIFTLEFT(word_count, 1) AS ShiftLeft",
                "SHIFTRIGHT(word_count, 1) AS ShiftRight",
                "CASE WHEN word_count > 10 THEN 'frequent' WHEN word_count <= 10 AND word_count > 4 THEN 'normal' ELSE 'rare' END AS WordFrequency",
                "(SELECT MAX(word_count) from shakespeare) as MaxWordCount",
                "(SELECT MAX(word_count) from shakespeare WHERE word IN ('glass', 'augurs')) as MaxWordCountInWords",
                "COALESCE(NULL, NULL, NULL, word, NULL, 'Push', 'Down') as Coalesce",
                "IF(word_count = 10 and word = 'glass', 'working', 'not working') AS IfCondition",
                "-(word_count) AS UnaryMinus",
                "CAST(word_count + 1.99 as DECIMAL(17, 2)) / CAST(word_count + 2.99 as DECIMAL(17, 1)) < 0.9")
            .where("word_count = 10 and word = 'glass'")
            .orderBy("word_count");

    List<Row> result = df.collectAsList();
    Row r1 = result.get(0);
    assertThat(r1.get(0)).isEqualTo("glass"); // word
    assertThat(r1.get(1)).isEqualTo(10); // word_count
    assertThat(r1.get(2)).isEqualTo("10"); // word_count
    assertThat(r1.get(3)).isEqualTo(20); // SHIFTLEFT(word_count, 1)
    assertThat(r1.get(4)).isEqualTo(5); // SHIFTRIGHT(word_count, 1)
    assertThat(r1.get(5)).isEqualTo("normal"); // CASE WHEN
    assertThat(r1.get(6)).isEqualTo(995); // SCALAR SUBQUERY
    assertThat(r1.get(7)).isEqualTo(10); // SCALAR SUBQUERY WITH IN
    assertThat(r1.get(8)).isEqualTo("glass"); // COALESCE
    assertThat(r1.get(9)).isEqualTo("working"); // IF CONDITION
    assertThat(r1.get(10)).isEqualTo(-10); // UNARY MINUS
    assertThat(r1.get(11)).isEqualTo(false); // CHECKOVERFLOW
  }

  @Test
  public void testUnionQuery() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);

    df.createOrReplaceTempView("shakespeare");
    Dataset<Row> words_with_word_count_100 =
        spark.sql("SELECT word, word_count FROM shakespeare WHERE word_count = 100");
    Dataset<Row> words_with_word_count_150 =
        spark.sql("SELECT word, word_count FROM shakespeare WHERE word_count = 150");

    List<Row> unionList =
        words_with_word_count_100.union(words_with_word_count_150).collectAsList();
    List<Row> unionAllList =
        words_with_word_count_150.unionAll(words_with_word_count_100).collectAsList();
    List<Row> unionByNameList =
        words_with_word_count_100.unionByName(words_with_word_count_150).collectAsList();
    assertThat(unionList.size()).isGreaterThan(0);
    assertThat(unionList.get(0).get(1)).isAnyOf(100L, 150L);
    assertThat(unionAllList.size()).isGreaterThan(0);
    assertThat(unionAllList.get(0).get(1)).isAnyOf(100L, 150L);
    assertThat(unionByNameList.size()).isGreaterThan(0);
    assertThat(unionByNameList.get(0).get(1)).isAnyOf(100L, 150L);
  }

  @Test
  public void testBooleanExpressions() {
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
                    + "word, "
                    + "word LIKE '%las%' AS Contains, "
                    + "word LIKE '%lass' AS Ends_With, "
                    + "word LIKE 'gla%' AS Starts_With "
                    + "FROM shakespeare "
                    + "WHERE word IN ('glass', 'very_random_word') AND word_count != 99")
            .collectAsList();

    Row r1 = result.get(0);
    assertThat(r1.get(0)).isEqualTo("glass"); // word
    assertThat(r1.get(1)).isEqualTo(true); // contains
    assertThat(r1.get(2)).isEqualTo(true); // ends_With
    assertThat(r1.get(3)).isEqualTo(true); // starts_With
  }

  @Test
  public void testWindowStatements() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);

    df.createOrReplaceTempView("shakespeare");

    df =
        spark.sql(
            "SELECT "
                + "*, "
                + "ROW_NUMBER() OVER (PARTITION BY corpus ORDER BY corpus_date) as row_number, "
                + "RANK() OVER (PARTITION BY corpus ORDER BY corpus_date) as rank, "
                + "DENSE_RANK() OVER (PARTITION BY corpus ORDER BY corpus_date) as dense_rank, "
                + "PERCENT_RANK() OVER (PARTITION BY corpus ORDER BY corpus_date) as percent_rank, "
                + "AVG(word_count) OVER (PARTITION BY corpus) as word_count_avg_by_corpus, "
                + "COUNT(word) OVER (PARTITION BY corpus ORDER BY corpus_date) as num_of_words_in_corpus, "
                + "COUNT(word) OVER count_window as num_of_words_in_corpus_window_clause "
                + "FROM shakespeare "
                + "WINDOW count_window AS (PARTITION BY corpus ORDER BY corpus_date)");
    /**
     * The reason I am filtering the dataframe later instead of adding where clause to the sql query
     * is, in SQL the window statement would be executed after the where clause filtering is done.
     * In order to test the appropriate behaviour, added the filtering port later.
     */
    Object[] filteredRow =
        df.collectAsList().stream()
            .filter(row -> row.get(0).equals("augurs") && row.get(2).equals("sonnets"))
            .toArray();
    assertThat(filteredRow.length).isEqualTo(1);
    GenericRowWithSchema row = (GenericRowWithSchema) filteredRow[0];
    assertThat(row.get(4))
        .isEqualTo(2); // ROW_NUMBER() OVER (PARTITION BY corpus ORDER BY corpus_date)
    assertThat(row.get(5)).isEqualTo(1); // RANK() OVER (PARTITION BY corpus ORDER BY corpus_date)
    assertThat(row.get(6))
        .isEqualTo(1); // DENSE_RANK() OVER (PARTITION BY corpus ORDER BY corpus_date)
    assertThat(row.get(7))
        .isEqualTo(0.0); // PERCENT_RANK() OVER (PARTITION BY corpus ORDER BY corpus_date)
    assertThat(row.get(8))
        .isEqualTo(4.842262714169159); // AVG(word_count) OVER (PARTITION BY corpus)
    assertThat(row.get(9))
        .isEqualTo(3677); // COUNT(word) OVER (PARTITION BY corpus ORDER BY corpus_date)
    assertThat(row.get(10)).isEqualTo(3677); // COUNT(word) OVER count_window
  }

  /** Method to create a test table of schema NumStruct, in test dataset */
  protected void writeTestDatasetToBigQuery() {
    Dataset<Row> df =
        spark
            .createDataset(
                Arrays.asList(
                    new NumStruct(
                        2L,
                        2L,
                        3L,
                        ImmutableList.of(new StringStruct("1:str3", "2:str1", "3:str2"))),
                    new NumStruct(
                        3L,
                        2L,
                        4L,
                        ImmutableList.of(new StringStruct("2:str3", "3:str1", "4:str2"))),
                    new NumStruct(
                        4L,
                        3L,
                        5L,
                        ImmutableList.of(new StringStruct("2:str3", "3:str1", "4:str2")))),
                Encoders.bean(NumStruct.class))
            .toDF();
    df.write()
        .format("bigquery")
        .mode(SaveMode.Append)
        .option("table", testDataset.toString() + "." + testTable)
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
        .option("writeMethod", WriteMethod.INDIRECT.toString())
        .save();
  }

  /** Method to create a test table of schema NumStruct, in test dataset */
  protected void writeTestDatasetToBigQuery2() {

    List<StringStruct> stringStructList = Arrays.asList(new StringStruct("absac", "dsd", "wewe"), new StringStruct("rthrt", "vdfv", "h"), new StringStruct("gtr", "sd", "fsfwefwefd"));
    Dataset<Row> df =
        spark
            .createDataset(stringStructList, Encoders.bean(StringStruct.class))
            .toDF();
    df.write()
        .format("bigquery")
        .mode(SaveMode.Append)
        .option("table", testDataset.toString() + "." + testTable)
        .option("temporaryGcsBucket", TestConstants.TEMPORARY_GCS_BUCKET)
        .option("writeMethod", WriteMethod.INDIRECT.toString())
        .save();
  }

  @Test
  public void testOrderBy() {
    writeTestDatasetToBigQuery();
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(testDataset.toString() + "." + testTable);

    df.createOrReplaceTempView("numStructDF");

    // List<Row> result =
    //     spark
    //         .sql(
    //             "SELECT num1, num2, num3 FROM numStructDF WHERE num3 > 2 AND num3 < 5 ORDER BY
    // num3 DESC LIMIT 1")
    //         .collectAsList();

    df =
        spark.sql(
            "SELECT num1, num2, num3 FROM numStructDF WHERE num3 > 2 AND num3 < 5 ORDER BY num3 LIMIT 5");

    // df.explain(true);
    // df.collect();
    df.show();
    // result.get(0);
  }

  @Test
  public void testOrderBy2() {
    writeTestDatasetToBigQuery2();
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(testDataset.toString() + "." + testTable);

    df.createOrReplaceTempView("numStructDF");

    // List<Row> result =
    //     spark
    //         .sql(
    //             "SELECT num1, num2, num3 FROM numStructDF WHERE num3 > 2 AND num3 < 5 ORDER BY
    // num3 DESC LIMIT 1")
    //         .collectAsList();

    df =
        spark.sql(
            "SELECT str1, str2, str3 FROM numStructDF WHERE str1 > 'd' ORDER BY str1 LIMIT 5");

    // df.explain(true);
    //df.collect();
    df.show();
    // result.get(0);
  }


  @Test
  public void testAggregation() {
    // spark.range(1, 100).createOrReplaceTempView("t1");
    // Dataset<Row> df = spark.sql("select id from t1 where t1.id = 10");
    // df.explain(true);

    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .option("table", "google.com:hadoop-cloud-dev:vinaylondhe_test.roster")
            .option("pushdownEnabled", true)
            .load()
            .where("_SchoolID >= 51 and _SchoolID <= 75");
    // .groupBy("LastName")
    // .sum("_SchoolID");

    // df.show();
    // df.groupBy("_SchoolID").sum().show();

    // df.groupBy(col("_SchoolID").as("SId")).agg(sum("_SchoolID").as("sum")).show();
    //

    // df.groupBy("O_CUSTKEY")
    //     .agg(sum("O_TOTALPRICE").alias("PRICE_NAME"))
    //     .sort(col("PRICE_NAME").desc)
    //     .limit(10)
    Dataset<Row> result = df.groupBy("LastName").sum("_SchoolID").sort("LastName");
    //Dataset<Row> result = df.sort("LastName");
    // // Row[] rowResult = (Row[]) result.take(5);
    result.show();
    // result.collect();

    // Dataset<Row> result2 = df.sort("LastName").limit(4);
    // result2.show(4);
    // df.sort("LastName").take(10);

    // df.sort("LastName").show(10);
    // Row[] rowResult = (Row[]) df.groupBy("LastName").sum("_SchoolID").sort("LastName").collect();
    // System.out.println(Arrays.toString(rowResult));
  }

  @Test
  public void testTpcDs() {
    String[] tables =
        new String[] {
          "catalog_sales",
          "catalog_returns",
          "date_dim",
          "time_dim",
          "customer",
          "customer_address",
          "customer_demographics",
          "household_demographics",
          "income_band",
          "household_demographics",
          "income_band",
          "inventory",
          "item",
          "promotion",
          "warehouse",
          "web_sales",
          "web_returns",
          "store",
          "web_page",
          "store_sales",
          "store_returns"
        };

    Dataset<Row> df;
    for (String table : tables) {
      df =
          spark
              .read()
              .format("bigquery")
              .option("table", "tpcds_1T." + table)
              .option("materializationDataset", testDataset.toString())
              .load();
      df.createOrReplaceTempView(table);
    }

    df =
        spark.sql(
            "select i_item_id,\n"
                + "        ca_country,\n"
                + "        ca_state,\n"
                + "        ca_county,\n"
                + "        avg( cast(cs_quantity as decimal(12,2))) agg1,\n"
                + "        avg( cast(cs_list_price as decimal(12,2))) agg2,\n"
                + "        avg( cast(cs_coupon_amt as decimal(12,2))) agg3,\n"
                + "        avg( cast(cs_sales_price as decimal(12,2))) agg4,\n"
                + "        avg( cast(cs_net_profit as decimal(12,2))) agg5,\n"
                + "        avg( cast(c_birth_year as decimal(12,2))) agg6,\n"
                + "        avg( cast(cd1.cd_dep_count as decimal(12,2))) agg7\n"
                + " from catalog_sales, customer_demographics cd1,\n"
                + "      customer_demographics cd2, customer, customer_address, date_dim, item\n"
                + " where cs_sold_date_sk = d_date_sk and\n"
                + "       cs_item_sk = i_item_sk and\n"
                + "       cs_bill_cdemo_sk = cd1.cd_demo_sk and\n"
                + "       cs_bill_customer_sk = c_customer_sk and\n"
                + "       cd1.cd_gender = 'F' and\n"
                + "       cd1.cd_education_status = 'Unknown' and\n"
                + "       c_current_cdemo_sk = cd2.cd_demo_sk and\n"
                + "       c_current_addr_sk = ca_address_sk and\n"
                + "       c_birth_month in (1,6,8,9,12,2) and\n"
                + "       d_year = 1998 and\n"
                + "       ca_state  in ('MS','IN','ND','OK','NM','VA','MS')\n"
                + " group by rollup (i_item_id, ca_country, ca_state, ca_county)\n"
                + " order by ca_country, ca_state, ca_county, i_item_id\n"
                + " LIMIT 100");

    // df.limit(10);
    // df.show();

    // df.sort("dt.d_year", "sum_agg", "brand_id").show(10);
    df.show();
  }
}
