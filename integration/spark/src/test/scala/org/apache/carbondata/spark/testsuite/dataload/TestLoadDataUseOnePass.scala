/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.carbondata.processing.etl.DataLoadingException
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestLoadDataUseOnePass extends QueryTest with BeforeAndAfterAll{
  override def beforeAll {
    sql("DROP TABLE IF EXISTS t3")
    sql("DROP TABLE IF EXISTS t3_onepass")
    sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'carbondata'
           """)

    // Load data without kettle
    sql(s"""
           LOAD DATA LOCAL INPATH 'E:/workspace/master/examples/src/main/resources/data.csv' into table t3
           OPTIONS('USE_KETTLE'='false', 'USEONEPASS'='false')
           """)

    sql("""
           CREATE TABLE IF NOT EXISTS t3_onepass
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'carbondata'
        """)

    // Load data without kettle, use one pass
    sql(s"""
           LOAD DATA LOCAL INPATH 'E:/workspace/master/examples/src/main/resources/data.csv' into table t3_onepass
           OPTIONS('USE_KETTLE'='false', 'USEONEPASS'='true')
           """)
  }

  test("test load data use one pass load") {
    checkAnswer(sql("select * from t3"), sql("select * from t3_onepass"))
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS t3")
    sql("DROP TABLE IF EXISTS t3_onepass")
  }
}
