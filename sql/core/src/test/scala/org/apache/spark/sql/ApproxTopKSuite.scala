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
class ApproxTopKSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  test("SPARK-xxxxx: test parameters") {
    // Integer
    val res1 = sql(
      "SELECT approx_top_k(expr) FROM VALUES (0), (0), (1), (1), (2), (3), (4), (4) AS tab(expr);"
    )
    checkAnswer(res1, Row(Seq(Row(0, 2), Row(4, 2), Row(1, 2), Row(2, 1), Row(3, 1))))

    // String
    val res2 = sql(
      "SELECT approx_top_k(expr, 2)" +
        "FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' AS tab(expr);")
    checkAnswer(res2, Row(Seq(Row("c", 4), Row("d", 2))))

    val res3 = sql(
      "SELECT approx_top_k(expr, 10, 100) FROM VALUES (0), (1), (1), (2), (2), (2) AS tab(expr);"
    )
    checkAnswer(res3, Row(Seq(Row(2, 3), Row(1, 2), Row(0, 1))))
  }

  test("SPARK-xxxxx: test for integer type") {
    // Integer
    val res1 = sql(
      "SELECT approx_top_k(expr) FROM VALUES (0), (0), (1), (1), (2), (3), (4), (4) AS tab(expr);"
    )
    checkAnswer(res1, Row(Seq(Row(0, 2), Row(4, 2), Row(1, 2), Row(2, 1), Row(3, 1))))
  }

  test("SPARK-xxxxx: test for string type") {
    // String
    val res2 = sql(
      "SELECT approx_top_k(expr, 2)" +
        "FROM VALUES 'a', 'b', 'c', 'c', 'c', 'c', 'd', 'd' AS tab(expr);")
    checkAnswer(res2, Row(Seq(Row("c", 4), Row("d", 2))))
  }

  test("SPARK-xxxxx: test for boolean type") {
    // Boolean
    val dfBool = Seq(true, true, false, true, true, false, false).toDF("expr")
    dfBool.createOrReplaceTempView("t_bool")
    val res4 = sql("SELECT approx_top_k(expr, 1) FROM t_bool;")
    checkAnswer(res4, Row(Seq(Row(true, 4))))
  }

  test("SPARK-xxxxx: test for byte type") {
    // Byte
    val res5 = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast(0 AS BYTE), cast(0 AS BYTE), cast(1 AS BYTE), cast(1 AS BYTE), " +
        "cast(2 AS BYTE), cast(3 AS BYTE), cast(4 AS BYTE), cast(4 AS BYTE) AS tab(expr);")
    checkAnswer(res5, Row(Seq(Row(0, 2), Row(4, 2))))
  }

  test("SPARK-xxxxx: test for short type") {
    // Short
    val res6 = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast(0 AS SHORT), cast(0 AS SHORT), cast(1 AS SHORT), cast(1 AS SHORT), " +
        "cast(2 AS SHORT), cast(3 AS SHORT), cast(4 AS SHORT), cast(4 AS SHORT) AS tab(expr);")
    checkAnswer(res6, Row(Seq(Row(0, 2), Row(4, 2))))
  }

  test("SPARK-xxxxx: test for long type") {
    // Long
    val res7 = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast(0 AS LONG), cast(0 AS LONG), cast(1 AS LONG), cast(1 AS LONG), " +
        "cast(2 AS LONG), cast(3 AS LONG), cast(4 AS LONG), cast(4 AS LONG) AS tab(expr);")
    checkAnswer(res7, Row(Seq(Row(0, 2), Row(4, 2))))
  }

  test("SPARK-xxxxx: test for float type") {
    // Float
    val res8 = sql(
      "SELECT approx_top_k(expr) " +
        "FROM VALUES cast(0.0 AS FLOAT), cast(0.0 AS FLOAT), " +
        "cast(1.0 AS FLOAT), cast(1.0 AS FLOAT), " +
        "cast(2.0 AS FLOAT), cast(3.0 AS FLOAT), " +
        "cast(4.0 AS FLOAT), cast(4.0 AS FLOAT) AS tab(expr);")
    checkAnswer(res8, Row(Seq(Row(0.0, 2), Row(1.0, 2), Row(4.0, 2), Row(2.0, 1), Row(3.0, 1))))
  }

  test("SPARK-xxxxx: test for double type") {
    // Double
    val res9 = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast(0.0 AS DOUBLE), cast(0.0 AS DOUBLE), " +
        "cast(1.0 AS DOUBLE), cast(1.0 AS DOUBLE), " +
        "cast(2.0 AS DOUBLE), cast(3.0 AS DOUBLE), " +
        "cast(4.0 AS DOUBLE), cast(4.0 AS DOUBLE) AS tab(expr);")
    checkAnswer(res9, Row(Seq(Row(0.0, 2), Row(4.0, 2))))
  }

  test("SPARK-xxxxx: test for date type") {
    // Date
    val res10 = sql(
      "SELECT approx_top_k(expr, 2) " +
        "FROM VALUES cast('2023-01-01' AS DATE), cast('2023-01-01' AS DATE), " +
        "cast('2023-01-02' AS DATE), cast('2023-01-02' AS DATE), " +
        "cast('2023-01-03' AS DATE), cast('2023-01-04' AS DATE), " +
        "cast('2023-01-05' AS DATE), cast('2023-01-05' AS DATE) AS tab(expr);")
    checkAnswer(
      res10,
      Row(Seq(Row(Date.valueOf("2023-01-02"), 2), Row(Date.valueOf("2023-01-01"), 2))))
  }

  test("SPARK-xxxxx: test for timestamp type") {
    // Timestamp
    val res11 = sql(
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
      res11,
      Row(Seq(Row(Timestamp.valueOf("2023-01-02 00:00:00"), 2),
        Row(Timestamp.valueOf("2023-01-05 00:00:00"), 2))))
  }

  test("SPARK-xxxxx: test for decimal type") {
    // Decimal
    val res12 = sql(
      "SELECT approx_top_k(expr, 2) AS top_k_result " +
        "FROM VALUES (0.0), (0.0), (0.0) ,(1.0), (1.0), (2.0), (3.0), (4.0) AS tab(expr);")
    checkAnswer(
      res12,
      Row(Seq(Row(new java.math.BigDecimal("0.0"), 3), Row(new java.math.BigDecimal("1.0"), 2))))
  }

  test("SPARK-xxxxx: test for decimal(10, 2) type") {
    // Decimal
    val res13 = sql(
      "SELECT approx_top_k(expr, 2) AS top_k_result " +
        "FROM VALUES CAST(0.0 AS DECIMAL(10, 2)), CAST(0.0 AS DECIMAL(10, 2)), " +
        "CAST(0.0 AS DECIMAL(10, 2)), CAST(1.0 AS DECIMAL(10, 2)), " +
        "CAST(1.0 AS DECIMAL(10, 2)), CAST(2.0 AS DECIMAL(10, 2)), " +
        "CAST(3.0 AS DECIMAL(10, 2)), CAST(4.0 AS DECIMAL(10, 2)) AS tab(expr);")
    checkAnswer(
      res13,
      Row(Seq(Row(new java.math.BigDecimal("0.00"), 3), Row(new java.math.BigDecimal("1.00"), 2))))
  }

  test("SPARK-xxxxx: test for decimal(20, 1) type") {
    // Decimal
    val res14 = sql(
      "SELECT approx_top_k(expr, 2) AS top_k_result " +
        "FROM VALUES CAST(0.0 AS DECIMAL(20, 1)), CAST(0.0 AS DECIMAL(20, 1)), " +
        "CAST(0.0 AS DECIMAL(20, 1)), CAST(1.0 AS DECIMAL(20, 1)), " +
        "CAST(1.0 AS DECIMAL(20, 1)), CAST(2.0 AS DECIMAL(20, 1)), " +
        "CAST(3.0 AS DECIMAL(20, 1)), CAST(4.0 AS DECIMAL(20, 1)) AS tab(expr);")
    checkAnswer(
      res14,
      Row(Seq(Row(new java.math.BigDecimal("0.0"), 3), Row(new java.math.BigDecimal("1.0"), 2))))
  }
}
