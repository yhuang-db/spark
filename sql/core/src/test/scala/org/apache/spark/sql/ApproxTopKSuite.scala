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

  test("SPARK-ace: test of accumulate and estimate") {
    val res1 = sql("SELECT approx_top_k_accumulate(expr) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (3), (4) AS tab(expr);")
    res1.show(truncate = false)

    res1.createOrReplaceTempView("accumulation")
    val res2 = sql("SELECT approx_top_k_estimate(acc) FROM accumulation;")
    res2.show(truncate = false)
  }

  test("SPARK-ace: accumulate and estimate test of String type") {
    val res = sql(
      "SELECT approx_top_k_estimate(approx_top_k_accumulate(expr), 2)" +
        "FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' AS tab(expr);")
    res.show(truncate = false)
    checkAnswer(res, Row(Seq(Row("c", 4), Row("d", 2))))
  }

  test("SPARK-combine1: test of accumulate, combine and estimate") {
    val res1 = sql("SELECT approx_top_k_accumulate(expr) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (3), (4) AS tab(expr);")
    res1.show(truncate = false)
    res1.createOrReplaceTempView("accumulation1")

    val res2 = sql("SELECT approx_top_k_accumulate(expr) as acc " +
      "FROM VALUES (1), (1), (2), (2), (3), (3), (4), (4) AS tab(expr);")
    res2.show(truncate = false)
    res2.createOrReplaceTempView("accumulation2")

    val res3 = sql("SELECT approx_top_k_combine(acc) as com " +
      "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
    res3.show(truncate = false)
    res3.createOrReplaceTempView("combined")

    val res4 = sql("SELECT approx_top_k_estimate(com) FROM combined;")
    res4.show(truncate = false)
  }

  test("SPARK-combine2: test of accumulate, combine and estimate 2") {
    val res1 = sql("SELECT approx_top_k_accumulate(expr) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (2), (3) AS tab(expr);")
    res1.show(truncate = false)
    res1.createOrReplaceTempView("accumulation1")

    val res2 = sql("SELECT approx_top_k_accumulate(expr) as acc " +
      "FROM VALUES (1), (1), (2), (2), (3), (3), (4), (4) AS tab(expr);")
    res2.show(truncate = false)
    res2.createOrReplaceTempView("accumulation2")

    val res3 = sql("SELECT approx_top_k_combine(acc) as com " +
      "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
    res3.show(truncate = false)
    res3.createOrReplaceTempView("combined")

    val res4 = sql("SELECT approx_top_k_estimate(com) FROM combined;")
    res4.show(truncate = false)
  }

  test("SPARK-combine3: test of accumulate, combine and estimate 3") {
    val res1 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (2), (3) AS tab(expr);")
    res1.show(truncate = false)
    res1.createOrReplaceTempView("accumulation1")

    val res2 = sql("SELECT approx_top_k_accumulate(expr, 20) as acc " +
      "FROM VALUES (1), (1), (2), (2), (3), (3), (4), (4) AS tab(expr);")
    res2.show(truncate = false)
    res2.createOrReplaceTempView("accumulation2")

    val res3 = sql("SELECT approx_top_k_combine(acc, 30) as com " +
      "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
    res3.show(truncate = false)
    res3.createOrReplaceTempView("combined")

    val res4 = sql("SELECT approx_top_k_estimate(com) FROM combined;")
    res4.show(truncate = false)
  }

  test("SPARK-combine4: test of accumulate, combine and estimate 4") {
    val res1 = sql("SELECT approx_top_k_accumulate(expr, 10) as acc " +
      "FROM VALUES (0), (0), (0), (1), (1), (2), (2), (3) AS tab(expr);")
    res1.show(truncate = false)
    res1.createOrReplaceTempView("accumulation1")

    val res2 = sql("SELECT approx_top_k_accumulate(expr, 20) as acc " +
      "FROM VALUES (1), (1), (2), (2), (3), (3), (4), (4) AS tab(expr);")
    res2.show(truncate = false)
    res2.createOrReplaceTempView("accumulation2")

    val res3 = sql("SELECT approx_top_k_combine(acc) as com " +
      "FROM (SELECT acc from accumulation1 UNION ALL SELECT acc FROM accumulation2);")
    res3.show(truncate = false)
    res3.createOrReplaceTempView("combined")

    val res4 = sql("SELECT approx_top_k_estimate(com) FROM combined;")
    res4.show(truncate = false)
  }
}
