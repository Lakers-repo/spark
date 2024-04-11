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

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.catalyst.plans.logical._

class HaxiPlanParserSuite extends AnalysisTest {
  import CatalystSqlParser._
  test("parsePlan") {
    /* val plan = parsePlan("select tmp1.id,tmp1.cell_type,tmp2.sq from (SELECT id, cell_type " +
      "from window_test_table  where rank>=2) tmp1 join (select sq,id from test1 where num>3)" +
      " tmp2 on tmp1.id=tmp2.id") */
//     val plan = parsePlan("SELECT a, b from src where c>=2")
    //  val plan = parsePlan("SELECT A,B, COUNT(1) AS cnt FROM TESTDATA2 WHERE A>2 GROUP BY A,B")

    // val plan = parsePlan("SELECT A,B ,CT FROM TESTDATA2 AS TMP1 ")
    // val plan = parsePlan("SELECT COL1, COL2 FROM TESTDATA AS TMP1(COL1, COL2); ")

    // val plan = parsePlan("SELECT T1.COL1, T2.COL2 FROM TESTDATA T1 JOIN TESTDATA1 T2 ON T1.ID
    // =T2.ID ")
    // val plan = parsePlan("SELECT T1.COL1, T2.COL2 FROM TESTDATA T1,T2  ")这种情况得讨论一下
    // val plan = parsePlan("SELECT T1.COL1, T2.COL2 FROM TESTDATA T1,T2 WHERE T1.ID=T2.ID
    // ") 这种情况得讨论一下
    // val plan = parsePlan("SELECT T1.COL1, T2.COL2 FROM TESTDATA T1 JOIN TESTDATA1 T2
    // ON T1.`(id)?+.+`=T2.ID")
    // val plan = parsePlan("SELECT T1.COL1,T1.COL2 FROM TESTDATA T1 LATERAL VIEW EXPLODE(T1.ID)
    // ID_TABLE1 AS TID,TID1")
    // val plan = parsePlan("SELECT `(id)?+.+` FROM TESTDATA")

    //  val plan = parsePlan("SELECT COL1, COL2  FROM TESTDATA2 TABLESAMPLE(10 ROWS) ")
    //  val plan = parsePlan("SELECT COL1, COL2  FROM TESTDATA2 TABLESAMPLE(0.1 PERCENT) ) ")
    //  val plan = parsePlan("SELECT COL1, COL2  FROM TESTDATA2 TABLESAMPLE(BUCKET 3 OUT OF 10 ) ")
    // val plan = parsePlan("SELECT A,B ,CT FROM TESTDATA2 TABLESAMPLE(BUCKET 3 OUT OF 10  ON CT)")
    val plan = parsePlan("SELECT A,B FROM TESTDATA2")
    // val plan = parsePlan("SELECT A,B,COUNT(*) AS CNT FROM TESTDATA2 WHERE A>2 GROUP BY A,B")
    // scalastyle:off println
    println()
    println("--------unresolved logical plan--------" )
    println(plan)
    println(plan.prettyJson)
    println("--------unresolved logical plan--------" )
    /* println(plan.nodeName)
    println(plan.children)
    println(plan.productArity)
    println(plan.productElement(0))
    println(plan.productElement(1))
    plan.productElement(1).asInstanceOf[LogicalPlan].productIterator foreach println
    println("----------------" + plan.productElement(1).asInstanceOf[LogicalPlan].nodeName)
    println("----------------" + plan.productElement(1).asInstanceOf[LogicalPlan].children)
    println("================" + plan.productElement(1).asInstanceOf[LogicalPlan].productElement(1))
    println("++++++++++++++++" + plan.productElement(1).asInstanceOf[LogicalPlan].productElement(1).
      asInstanceOf[LogicalPlan].nodeName)
    val eplan = plan.productElement(1).asInstanceOf[LogicalPlan].productElement(1).
      asInstanceOf[LogicalPlan]
    println("sssssssssssssss" + plan.childrenResolved)
    println("++++++++++++++++" + eplan.children)
    println("XXXXXXXXXXX" + plan.containsChild)
    println("XXXXXXXXXXX" + plan.containsChild(eplan))
    println("22222222222222" + plan.productElement(1).asInstanceOf[LogicalPlan]
      .containsChild(eplan))
    println("***********")
    println(plan.origin.productArity)
    println(plan.origin.productElement(0))
    println(plan.origin.productElement(1)) */
    // println("11111111111111111")
    // println(mapProductIterator(plan))
    // plan.mapProductIterator{ case arg: TreeNode[_] => println(arg) }
    // println("--------unresolved logical plan--------" )
    // println(plan)
    // scalastyle:on println
  }

  def mapProductIterator ( plan : LogicalPlan ): Array[Any] = {
    val arr = Array.ofDim[Any](plan.productArity)
    var i = 0
    while (i < arr.length) {
      arr(i) = plan.productElement(i)
      i += 1
    }
    arr
  }
}