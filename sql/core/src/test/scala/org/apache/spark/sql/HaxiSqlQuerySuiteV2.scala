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

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.util.Utils
import java.sql.{Connection, DriverManager}
import java.util.Properties

class HaxiSqlQuerySuiteV2 extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  setupTestData()

  val tempDir = Utils.createTempDir()
  val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"
  val defaultMetadata = new MetadataBuilder().putLong("scale", 0).build()
  var conn: java.sql.Connection = null

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.h2", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.h2.url", url)
    .set("spark.sql.catalog.h2.driver", "org.h2.Driver")

  private def withConnection[T](f: Connection => T): T = {
    val conn = DriverManager.getConnection(url, new Properties())
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Utils.classForName("org.h2.Driver")
    withConnection { conn =>
      conn.prepareStatement("""CREATE SCHEMA "test"""").executeUpdate()
      conn.prepareStatement(
        """CREATE TABLE "test"."people" (name TEXT(32) NOT NULL, id INTEGER NOT NULL)""")
        .executeUpdate()
      conn.prepareStatement(
        """insert into "test"."people" (name , id ) values ('jack',1),('tom',2)""")
        .executeUpdate()
    }
  }

  test("logical test") {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // val analyzedDF = sql("SHOW TABLES IN h2.test")
    val analyzedDF = sql("select name,id from h2.test.people where id>1")
    // val analyzedDF = sql("select * from testdata2")
    // val analyzedDF=sql( "select t1.*,t2.*  from testdata2 t1 join  h2.test.people t2 on t1.a=t2.id")
    val ana = analyzedDF.queryExecution.analyzed
    println("== Analyzed Logical Plan ==" )
    println( ana)
    // println( ana.prettyJson)
    println("== Optimized Logical Plan ==" )
    val opt=analyzedDF.queryExecution.optimizedPlan
    println(opt)
    // println( opt.prettyJson)
    println("== Physical Plan ==" )
    val  phy= analyzedDF.queryExecution.sparkPlan
    println(phy)
    // println( phy.prettyJson)
    println("== executedPlan ==" )
    println(analyzedDF.queryExecution.executedPlan)
    analyzedDF.show()
  }
}