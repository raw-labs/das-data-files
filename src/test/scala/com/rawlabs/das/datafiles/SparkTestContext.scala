/*
 * Copyright 2025 RAW Labs S.A.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0, included in the file
 * licenses/APL.txt.
 */

package com.rawlabs.das.datafiles
import org.apache.spark.sql.SparkSession

trait SparkTestContext {

  private val extraJvmOptions = Seq(
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED")

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .appName("TestSpark")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.extraJavaOptions", extraJvmOptions.mkString(" "))
      .config("spark.executor.extraJavaOptions", extraJvmOptions.mkString(" "))
      .getOrCreate()
  }
}
