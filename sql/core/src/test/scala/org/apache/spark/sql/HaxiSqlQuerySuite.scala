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

import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession

class HaxiSqlQuerySuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  setupTestData()

  test("common entry") {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val df = spark.read.json("examples/src/main/resources/people.json")
    //    df.cache()
    df.createOrReplaceTempView("people")
    val sqlDF1 = spark.sql("SELECT * FROM people a join people b on a.name = b.name")
    println("sqlDF1 spark plan ==========")
    println(sqlDF1.queryExecution.sparkPlan)
    println("sqlDF1 execute plan ==========")
    println(sqlDF1.queryExecution.executedPlan)
        sqlDF1.show()
  }

  test("logical test") {
    //    val analyzedDF = sql("select b, count(a) as a1, sum(a+b) as a2, sum(b+a) as a3,sum(a)+1 as a4 from testdata2 where b<2 group by b")
    //    val analyzedDF = sql("select a from (select a,b from testdata2 where a>2 ) tmp")
    //    val analyzedDF = sql("SELECT A,COUNT(*) AS CNT FROM TESTDATA2 GROUP BY A")
    //    val analyzedDF = sql("SELECT A,B FROM TESTDATA2")
//    val analyzedDF = sql("select tmp.A from (select A,B from testdata2) tmp")
//    val analyzedDF = sql("select a,b from testdata2  group by a,b")
//    val analyzedDF = sql("select * from testdata2 where a not in (select b from testdata2)")
//    val analyzedDF = sql("select * from testdata2 where a in (select b from testdata2)")
        val analyzedDF = sql("select tmp1.A, tmp2.B from testdata2 tmp1 join testdata2 tmp2 on tmp1.b = tmp2.b")
    //    val analyzedDF = sql("select a, count(b), sum(b) from testdata2 group by a")
    //    analyzedDF.cache()
    //    val analyzedDF = sql("select A from testdata2 where B > 1")
    println("== Analyzed Logical Plan ==")
    val ana = analyzedDF.queryExecution.analyzed
    println(ana)
    // println( ana.prettyJson)
    println("== Optimized Logical Plan ==")
    val opt = analyzedDF.queryExecution.optimizedPlan
    println(opt)
    // println( opt.prettyJson)
    println("== Physical Plan ==")
    val phy = analyzedDF.queryExecution.sparkPlan
    println(phy)
    // println(phy.prettyJson)
    println("== executedPlan ==")
    println(analyzedDF.queryExecution.executedPlan)
    // print generated code
//    println(analyzedDF.queryExecution.debug.codegen())
    analyzedDF.show()
//    println(analyzedDF.queryExecution.executedPlan.schema.fieldNames.mkString(" | "))
//    analyzedDF.queryExecution.toRdd.collect().map(
//      x => {
////        x.asInstanceOf[UnsafeRow]
//      }
//    )
  }

  test("SPARK-19059: read file based table whose name starts with underscore") {
    withTable("_tbl") {
      sql("CREATE TABLE `_tbl`(i INT) USING parquet")
      //val analyzedDF =sql("INSERT INTO `_tbl` VALUES (1), (2), (3)")
      val analyzedDF = sql("show create table _tbl")
      val ana = analyzedDF.queryExecution.analyzed
      println("== Analyzed Logical Plan ==")
      println(ana)
      // println( ana.prettyJson)
      println("== Optimized Logical Plan ==")
      val opt = analyzedDF.queryExecution.optimizedPlan
      println(opt)
      // println( opt.prettyJson)
      println("== Physical Plan ==")
      println(analyzedDF.queryExecution.sparkPlan)
      println("== executedPlan ==")
      println(analyzedDF.queryExecution.executedPlan)
    }
  }

  test("show the sql plan by explain command") {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val df = spark.read.json("examples/src/main/resources/people.json")
    df.createOrReplaceTempView("people")
    spark.sql("SELECT * FROM people").explain(true)
  }
}