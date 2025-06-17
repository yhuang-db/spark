/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.spark.sql

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.SlowSQLTest


@SlowSQLTest
class ApproxTopKSuite
  extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._


  test("SPARK-xxxxx: test of 1 parameter") {
    val res = sql(
      "SELECT approx_top_k(expr) FROM VALUES (0), (0), (1), (1), (2), (3), (4), (4) AS tab(expr);"
    )
    checkAnswer(res, Row(Seq(Row(0, 2), Row(4, 2), Row(1, 2), Row(2, 1), Row(3, 1))))
  }

  test("SPARK-xxxxx: test of 2 parameter") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' AS tab(expr);")
    checkAnswer(res, Row(Seq(Row("c", 4), Row("d", 2))))
  }

  test("SPARK-xxxxx: test of 3 parameter") {
    val res = sql(
      "SELECT approx_top_k(expr, 10, 100) FROM VALUES (0), (1), (1), (2), (2), (2) AS tab(expr);"
    )
    checkAnswer(res, Row(Seq(Row(2, 3), Row(1, 2), Row(0, 1))))
  }

  test("SPARK-xxxxx: test of Integer type") {
    val res = sql(
      "SELECT approx_top_k(expr) FROM VALUES (0), (0), (1), (1), (2), (3), (4), (4) AS tab(expr);"
    )
    checkAnswer(res, Row(Seq(Row(0, 2), Row(4, 2), Row(1, 2), Row(2, 1), Row(3, 1))))
  }

  test("SPARK-xxxxx: test of String type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2)" +
        "FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' AS tab(expr);")
    checkAnswer(res, Row(Seq(Row("c", 4), Row("d", 2))))
  }

  test("SPARK-xxxxx: test of Boolean type") {
    val dfBool = Seq(true, true, false, true, true, false, false).toDF("expr")
    dfBool.createOrReplaceTempView("t_bool")
    val res = sql("SELECT approx_top_k(expr, 1) FROM t_bool;")
    checkAnswer(res, Row(Seq(Row(true, 4))))
  }

  test("SPARK-xxxxx: test of Byte type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast(0 AS BYTE), cast(0 AS BYTE), cast(1 AS BYTE), cast(1 AS BYTE), " +
        "cast(2 AS BYTE), cast(3 AS BYTE), cast(4 AS BYTE), cast(4 AS BYTE) AS tab(expr);")
    checkAnswer(res, Row(Seq(Row(0, 2), Row(4, 2))))
  }

  test("SPARK-xxxxx: test of Short type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast(0 AS SHORT), cast(0 AS SHORT), cast(1 AS SHORT), cast(1 AS SHORT), " +
        "cast(2 AS SHORT), cast(3 AS SHORT), cast(4 AS SHORT), cast(4 AS SHORT) AS tab(expr);")
    checkAnswer(res, Row(Seq(Row(0, 2), Row(4, 2))))
  }

  test("SPARK-xxxxx: test of Long type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast(0 AS LONG), cast(0 AS LONG), cast(1 AS LONG), cast(1 AS LONG), " +
        "cast(2 AS LONG), cast(3 AS LONG), cast(4 AS LONG), cast(4 AS LONG) AS tab(expr);")
    checkAnswer(res, Row(Seq(Row(0, 2), Row(4, 2))))
  }

  test("SPARK-xxxxx: test of Float type") {
    val res = sql(
      "SELECT approx_top_k(expr) " +
        "FROM VALUES cast(0.0 AS FLOAT), cast(0.0 AS FLOAT), " +
        "cast(1.0 AS FLOAT), cast(1.0 AS FLOAT), " +
        "cast(2.0 AS FLOAT), cast(3.0 AS FLOAT), " +
        "cast(4.0 AS FLOAT), cast(4.0 AS FLOAT) AS tab(expr);")
    checkAnswer(res, Row(Seq(Row(0.0, 2), Row(1.0, 2), Row(4.0, 2), Row(2.0, 1), Row(3.0, 1))))
  }

  test("SPARK-xxxxx: test of Double type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast(0.0 AS DOUBLE), cast(0.0 AS DOUBLE), " +
        "cast(1.0 AS DOUBLE), cast(1.0 AS DOUBLE), " +
        "cast(2.0 AS DOUBLE), cast(3.0 AS DOUBLE), " +
        "cast(4.0 AS DOUBLE), cast(4.0 AS DOUBLE) AS tab(expr);")
    checkAnswer(res, Row(Seq(Row(0.0, 2), Row(4.0, 2))))
  }

  test("SPARK-xxxxx: test of Date type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast('2023-01-01' AS DATE), cast('2023-01-01' AS DATE), " +
        "cast('2023-01-02' AS DATE), cast('2023-01-02' AS DATE), " +
        "cast('2023-01-03' AS DATE), cast('2023-01-04' AS DATE), " +
        "cast('2023-01-05' AS DATE), cast('2023-01-05' AS DATE) AS tab(expr);")
    checkAnswer(
      res,
      Row(Seq(Row(Date.valueOf("2023-01-02"), 2), Row(Date.valueOf("2023-01-01"), 2))))
  }

  test("SPARK-xxxxx: test of Timestamp type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast('2023-01-01 00:00:00' AS TIMESTAMP), " +
        "cast('2023-01-01 00:00:00' AS TIMESTAMP), " +
        "cast('2023-01-02 00:00:00' AS TIMESTAMP), " +
        "cast('2023-01-02 00:00:00' AS TIMESTAMP), " +
        "cast('2023-01-03 00:00:00' AS TIMESTAMP), " +
        "cast('2023-01-04 00:00:00' AS TIMESTAMP), " +
        "cast('2023-01-05 00:00:00' AS TIMESTAMP), " +
        "cast('2023-01-05 00:00:00' AS TIMESTAMP) AS tab(expr);")
    checkAnswer(
      res,
      Row(Seq(Row(Timestamp.valueOf("2023-01-02 00:00:00"), 2),
        Row(Timestamp.valueOf("2023-01-05 00:00:00"), 2))))
  }

  test("SPARK-xxxxx: test of Decimal type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) AS top_k_result " +
        "FROM VALUES (0.0), (0.0), (0.0) ,(1.0), (1.0), (2.0), (3.0), (4.0) AS tab(expr);")
    checkAnswer(
      res,
      Row(Seq(Row(new java.math.BigDecimal("0.0"), 3), Row(new java.math.BigDecimal("1.0"), 2))))
  }

  test("SPARK-xxxxx: test of Decimal(10, 2) type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) AS top_k_result " +
        "FROM VALUES CAST(0.0 AS DECIMAL(10, 2)), CAST(0.0 AS DECIMAL(10, 2)), " +
        "CAST(0.0 AS DECIMAL(10, 2)), CAST(1.0 AS DECIMAL(10, 2)), " +
        "CAST(1.0 AS DECIMAL(10, 2)), CAST(2.0 AS DECIMAL(10, 2)), " +
        "CAST(3.0 AS DECIMAL(10, 2)), CAST(4.0 AS DECIMAL(10, 2)) AS tab(expr);")
    checkAnswer(
      res,
      Row(Seq(Row(new java.math.BigDecimal("0.00"), 3), Row(new java.math.BigDecimal("1.00"), 2))))
  }

  test("SPARK-xxxxx: test of Decimal(20, 1) type") {
    val res = sql(
      "SELECT approx_top_k(expr, 2) AS top_k_result " +
        "FROM VALUES CAST(0.0 AS DECIMAL(20, 1)), CAST(0.0 AS DECIMAL(20, 1)), " +
        "CAST(0.0 AS DECIMAL(20, 1)), CAST(1.0 AS DECIMAL(20, 1)), " +
        "CAST(1.0 AS DECIMAL(20, 1)), CAST(2.0 AS DECIMAL(20, 1)), " +
        "CAST(3.0 AS DECIMAL(20, 1)), CAST(4.0 AS DECIMAL(20, 1)) AS tab(expr);")
    checkAnswer(
      res,
      Row(Seq(Row(new java.math.BigDecimal("0.0"), 3), Row(new java.math.BigDecimal("1.0"), 2))))
  }

  test("SPARK-ace: accumulate and estimate test of Integer type") {
    val acc = sql("SELECT approx_top_k_accumulate(expr) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (3), (4) AS tab(expr);")
    acc.createOrReplaceTempView("accumulation")
    val res = sql("SELECT approx_top_k_estimate(acc) FROM accumulation;")
    checkAnswer(res, Row(Seq(Row(0, 3), Row(1, 2), Row(4, 1), Row(2, 1), Row(3, 1))))
  }

  test("SPARK-ace: accumulate and estimate test of String type") {
    val res = sql(
      "SELECT approx_top_k_estimate(approx_top_k_accumulate(expr), 2)" +
        "FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' AS tab(expr);")
    checkAnswer(res, Row(Seq(Row("c", 4), Row("d", 2))))
  }

  test("SPARK-ace: accumulate and estimate test of Decimal type") {
    val res = sql(
      "SELECT approx_top_k_estimate(approx_top_k_accumulate(expr), 2) " +
        "FROM VALUES (0.0), (0.0), (0.0) ,(1.0), (1.0), (2.0), (3.0), (4.0) AS tab(expr);")
    checkAnswer(
      res,
      Row(Seq(Row(new java.math.BigDecimal("0.0"), 3), Row(new java.math.BigDecimal("1.0"), 2))))
  }

  test("SPARK-combine: same type, same size, specified combine size - success") {
    val acc1 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (2), (3) AS tab(expr);")
    acc1.createOrReplaceTempView("accumulation1")

    val acc2 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES (1), (1), (2), (2), (3), (3), (4), (4) AS tab(expr);")
    acc2.createOrReplaceTempView("accumulation2")

    val comb = sql("SELECT approx_top_k_combine(acc, 30) as com " +
      "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
    comb.createOrReplaceTempView("combined")

    val est = sql("SELECT approx_top_k_estimate(com) FROM combined;")
    checkAnswer(est, Row(Seq(Row(2, 4), Row(1, 4), Row(0, 3), Row(3, 3), Row(4, 2))))
  }

  test("SPARK-combine: same type, same size, unspecified combine size - success") {
    val acc1 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (2), (3) AS tab(expr);")
    acc1.createOrReplaceTempView("accumulation1")

    val acc2 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES (1), (1), (2), (2), (3), (3), (4), (4) AS tab(expr);")
    acc2.createOrReplaceTempView("accumulation2")

    val comb = sql("SELECT approx_top_k_combine(acc) as com " +
      "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
    comb.createOrReplaceTempView("combined")

    val est = sql("SELECT approx_top_k_estimate(com) FROM combined;")
    checkAnswer(est, Row(Seq(Row(2, 4), Row(1, 4), Row(0, 3), Row(3, 3), Row(4, 2))))
  }

  test("SPARK-combine: same type, different size, specified combine size - success") {
    val acc1 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (2), (3) AS tab(expr);")
    acc1.createOrReplaceTempView("accumulation1")

    val acc2 = sql("SELECT approx_top_k_accumulate(expr, 20) as acc " +
      "FROM VALUES (1), (1), (2), (2), (3), (3), (4), (4) AS tab(expr);")
    acc2.createOrReplaceTempView("accumulation2")

    val comb = sql("SELECT approx_top_k_combine(acc, 30) as com " +
      "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
    comb.createOrReplaceTempView("combination")

    val est = sql("SELECT approx_top_k_estimate(com) FROM combination;")
    checkAnswer(est, Row(Seq(Row(2, 4), Row(1, 4), Row(0, 3), Row(3, 3), Row(4, 2))))
  }

  test("SPARK-combine: same type, different size, unspecified combine size - fail") {
    val acc1 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (2), (3) AS tab(expr);")
    acc1.createOrReplaceTempView("accumulation1")

    val acc2 = sql("SELECT approx_top_k_accumulate(expr, 20) as acc " +
      "FROM VALUES (1), (1), (2), (2), (3), (3), (4), (4) AS tab(expr);")
    acc2.createOrReplaceTempView("accumulation2")

    val comb = sql("SELECT approx_top_k_combine(acc) as com " +
      "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
    comb.createOrReplaceTempView("combination")

    val est = sql("SELECT approx_top_k_estimate(com) FROM combination;")

    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        est.collect()
      },
      condition = "APPROX_TOP_K_SKETCH_SIZE_UNMATCHED",
      parameters = Map("size1" -> "10", "size2" -> "20")
    )
  }

  test("SPARK-combine: different type (int VS string), same size, specified combine size - fail") {
    val acc1 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (2), (3) AS tab(expr);")
    acc1.createOrReplaceTempView("accumulation1")

    val acc2 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' AS tab(expr);")
    acc2.createOrReplaceTempView("accumulation2")

    val comb = sql("SELECT approx_top_k_combine(acc, 30) as com " +
      "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
    comb.createOrReplaceTempView("combined")

    val est = sql("SELECT approx_top_k_estimate(com) FROM combined;")
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        est.collect()
      },
      condition = "APPROX_TOP_K_SKETCH_TYPE_UNMATCHED")
  }

  test("SPARK-debug1: different type (int VS date), same size, specified combine size - fail") {
    val acc1 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (2), (3) AS tab(expr);")
    acc1.createOrReplaceTempView("accumulation1")

    val acc2 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES cast('2023-01-01' AS DATE), cast('2023-01-01' AS DATE), " +
      "cast('2023-01-02' AS DATE), cast('2023-01-02' AS DATE), " +
      "cast('2023-01-03' AS DATE), cast('2023-01-04' AS DATE), " +
      "cast('2023-01-05' AS DATE), cast('2023-01-05' AS DATE) AS tab(expr);")
    acc2.createOrReplaceTempView("accumulation2")

    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2;")
      },
      condition = "INCOMPATIBLE_COLUMN_TYPE",
      parameters = Map(
        "tableOrdinalNumber" -> "second",
        "columnOrdinalNumber" -> "first",
        "dataType2" -> ("\"STRUCT<DataSketch: BINARY NOT NULL, " +
          "ItemTypeNull: INT, MaxItemsTracked: INT NOT NULL>\""),
        "operator" -> "UNION",
        "hint" -> "",
        "dataType1" -> ("\"STRUCT<DataSketch: BINARY NOT NULL, " +
          "ItemTypeNull: DATE, MaxItemsTracked: INT NOT NULL>\"")
      )
    )
  }

  test("SPARK-combine: different type (int VS float), same size, specified combine size - fail") {
    val acc1 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (2), (3) AS tab(expr);")
    acc1.createOrReplaceTempView("accumulation1")

    val acc2 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES cast(0.0 AS FLOAT), cast(0.0 AS FLOAT), " +
      "cast(1.0 AS FLOAT), cast(1.0 AS FLOAT), " +
      "cast(2.0 AS FLOAT), cast(3.0 AS FLOAT), " +
      "cast(4.0 AS FLOAT), cast(4.0 AS FLOAT) AS tab(expr);")
    acc2.createOrReplaceTempView("accumulation2")

    val comb = sql("SELECT approx_top_k_combine(acc, 30) as com " +
      "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
    comb.createOrReplaceTempView("combined")

    val est = sql("SELECT approx_top_k_estimate(com) FROM combined;")
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        est.collect()
      },
      condition = "APPROX_TOP_K_SKETCH_TYPE_UNMATCHED")
  }

  test("SPARK-combine: different type (byte VS short), same size, specified combine size - fail") {
    val acc1 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES cast(0 AS BYTE), cast(0 AS BYTE), cast(1 AS BYTE), " +
      "cast(1 AS BYTE), cast(2 AS BYTE), cast(3 AS BYTE), " +
      "cast(4 AS BYTE), cast(4 AS BYTE) AS tab(expr);")
    acc1.createOrReplaceTempView("accumulation1")

    val acc2 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES cast(0 AS SHORT), cast(0 AS SHORT), cast(1 AS SHORT), " +
      "cast(1 AS SHORT), cast(2 AS SHORT), cast(3 AS SHORT), " +
      "cast(4 AS SHORT), cast(4 AS SHORT) AS tab(expr);")
    acc2.createOrReplaceTempView("accumulation2")

    val comb = sql("SELECT approx_top_k_combine(acc, 30) as com " +
      "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
    comb.createOrReplaceTempView("combined")

    val est = sql("SELECT approx_top_k_estimate(com) FROM combined;")
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        est.collect()
      },
      condition = "APPROX_TOP_K_SKETCH_TYPE_UNMATCHED")
  }

  test("SPARK-debug2: different type (decimal(10, 2) VS decimal(20, 3)), same size - fail") {
    val acc1 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES CAST(0.0 AS DECIMAL(10, 2)), CAST(0.0 AS DECIMAL(10, 2)), " +
      "CAST(1.0 AS DECIMAL(10, 2)), CAST(1.0 AS DECIMAL(10, 2)), " +
      "CAST(2.0 AS DECIMAL(10, 2)), CAST(3.0 AS DECIMAL(10, 2)), " +
      "CAST(4.0 AS DECIMAL(10, 2)), CAST(4.0 AS DECIMAL(10, 2)) AS tab(expr);")
    acc1.createOrReplaceTempView("accumulation1")

    val acc2 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES CAST(0.0 AS DECIMAL(20, 3)), CAST(0.0 AS DECIMAL(20, 3)), " +
      "CAST(1.0 AS DECIMAL(20, 3)), CAST(1.0 AS DECIMAL(20, 3)), " +
      "CAST(2.0 AS DECIMAL(20, 3)), CAST(3.0 AS DECIMAL(20, 3)), " +
      "CAST(4.0 AS DECIMAL(20, 3)), CAST(4.0 AS DECIMAL(20, 3)) AS tab(expr);")
    acc2.createOrReplaceTempView("accumulation2")

    val comb = sql("SELECT approx_top_k_combine(acc, 30) as com " +
      "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
    comb.createOrReplaceTempView("combined")

    val est = sql("SELECT approx_top_k_estimate(com) FROM combined;")
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        est.collect()
      },
      condition = "APPROX_TOP_K_SKETCH_TYPE_UNMATCHED")
  }

  test("SPARK-combin: test of accumulate, combine and estimate 5") {
    val res1 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (2), (3) AS tab(expr);")
    res1.show(truncate = false)
    res1.createOrReplaceTempView("accumulation1")

    val res2 = sql("SELECT approx_top_k_accumulate(expr, 20) as acc " +
      "FROM VALUES (1), (1), (2), (2), (3), (3), (4), (4) AS tab(expr);")
    res2.show(truncate = false)
    res2.createOrReplaceTempView("accumulation2")

    val res3 = sql("SELECT approx_top_k_accumulate(expr, 30) as acc " +
      "FROM VALUES (5), (5), (5), (5), (5), (0), (0), (0) AS tab(expr);")
    res3.show(truncate = false)
    res3.createOrReplaceTempView("accumulation3")

    val res4 = sql("SELECT approx_top_k_combine(acc) as com " +
      "FROM (SELECT acc from accumulation1 " +
      "UNION ALL SELECT acc FROM accumulation2 " +
      "UNION ALL SELECT acc FROM accumulation3);")
    res4.show(truncate = false)
    res4.createOrReplaceTempView("combined")

    val res5 = sql("SELECT approx_top_k_estimate(com) FROM combined;")
    res5.show(truncate = false)
  }

  test("SPARK-combin: test of accumulate, combine and estimate 6") {
    val acc1 = sql("SELECT approx_top_k_accumulate(expr) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (2), (3) AS tab(expr);")
    acc1.createOrReplaceTempView("accumulation1")

    val acc2 = sql("SELECT approx_top_k_accumulate(expr) as acc " +
      "FROM VALUES (1.0), (1.0), (2.0), (2.0), (3.0), (3.0), (4.0), (4.0) AS tab(expr);")
    acc2.createOrReplaceTempView("accumulation2")

    val comb = sql("SELECT approx_top_k_combine(acc) as com " +
      "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
    comb.createOrReplaceTempView("combination")

    comb.show()
  }

  test("SPARK-combin: test of accumulate, combine and estimate 7") {
    val acc1 = sql("SELECT approx_top_k_accumulate(expr) as acc " +
      "FROM VALUES (0.0), (0.0), (0.0), (1.0), (1.0), (2.0), (2.0), (3.0) AS tab(expr);")
    acc1.createOrReplaceTempView("accumulation1")

    val acc2 = sql("SELECT approx_top_k_accumulate(expr) as acc " +
      "FROM VALUES (1.0), (1.0), (2.0), (2.0), (3.0), (3.0), (4.0), (4.0) AS tab(expr);")
    acc2.createOrReplaceTempView("accumulation2")

    val comb = sql("SELECT approx_top_k_combine(acc) as com " +
      "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
    comb.createOrReplaceTempView("combination")

    comb.show()
  }

  test("SPARK-combin: test of accumulate, combine and estimate 8") {
    val acc1 = sql("SELECT approx_top_k_accumulate(expr) as acc " +
      "FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' AS tab(expr);")
    acc1.createOrReplaceTempView("accumulation1")

    val acc2 = sql("SELECT approx_top_k_accumulate(expr) as acc " +
      "FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' AS tab(expr);")
    acc2.createOrReplaceTempView("accumulation2")

    val comb = sql("SELECT approx_top_k_combine(acc) as com " +
      "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
    comb.createOrReplaceTempView("combination")

    val res = sql("SELECT approx_top_k_estimate(com) FROM combination;")
    res.show(truncate = false)
  }
}
