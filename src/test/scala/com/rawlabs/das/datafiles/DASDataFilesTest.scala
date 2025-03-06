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

// DASDataFilesSpec.scala
package com.rawlabs.das.datafiles

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.protocol.das.v1.tables.TableDefinition

class DASDataFilesTest extends AnyFlatSpec with Matchers with SparkTestContext {

  "DASDataFiles" should "create tables from config and read definitions" in {
    // 1) create a local CSV for testing
    val tempFile = java.io.File.createTempFile("integration-", ".csv")
    tempFile.deleteOnExit()
    val csvContent = "id,name\n1,Alice\n2,Bob\n"
    java.nio.file.Files.write(tempFile.toPath, csvContent.getBytes())

    // 2) Build plugin options
    val pluginOptions: Map[String, String] = Map(
      "nr_tables" -> "1",
      "table0_url" -> tempFile.getAbsolutePath,
      "table0_format" -> "csv",
      "table0_name" -> "my_csv",
      "table0_option_header" -> "true")

    // 3) Instantiate the plugin
    val das = new DASDataFiles(pluginOptions)

    // 4) Check table definitions
    val defs: Seq[TableDefinition] = das.tableDefinitions
    defs.size shouldBe 1
    defs.head.getTableId.getName shouldBe "my_csv"

    // 5) Check we can get the table
    val tableOpt = das.getTable("my_csv")
    tableOpt should not be empty

    val table = tableOpt.get

    val result = table.execute(Nil, Nil, Nil, None)
    result.hasNext shouldBe true
    val row1 = result.next()
    println(s"Row1: $row1")
    val row2 = result.next()
    println(s"Row2: $row2")
    result.hasNext shouldBe false
  }
}
