/*
 * Copyright 2024 RAW Labs S.A.
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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.datafiles.csv.CsvTable
import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSettings}

class DASDataFilesBuilderTest extends AnyFlatSpec with Matchers {

  // Provide a minimal implicit DASSettings for the build() calls
  implicit val settings: DASSettings = new DASSettings {}

  // ------------------------------------------------
  behavior of "DASDataFilesBuilder"

  it should "build a DASDataFiles that supports various formats from any local or non-specific URL" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "/local/path/file.csv", "table0_format" -> "csv")
    val sdk = new DASDataFilesBuilder().build(options)
    sdk shouldBe a[DASDataFiles]

    val dataFiles = sdk.asInstanceOf[DASDataFiles]
    dataFiles.tables.size shouldBe 1
    val (_, table) = dataFiles.tables.head
    table shouldBe a[CsvTable]
  }

  it should "throw an exception if format is missing for DASDataFiles" in {
    val options = Map(
      "nr_tables" -> "1",
      "table0_url" -> "/local/path/file.csv"
      // missing table0_format
    )
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASDataFilesBuilder().build(options)
    }
  }
}
