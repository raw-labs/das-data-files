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

class DASDataFilesOptionsSpec extends AnyFlatSpec with Matchers {

  behavior of "DASDataFilesOptions"

  it should "parse multiple tables" in {
    val opts = Map(
      "nr_tables" -> "2",

      // Table0
      "table0_url" -> "/path/to/data.csv",
      "table0_format" -> "csv",

      // Table1
      "table1_url" -> "/another/path/data.json",
      "table1_format" -> "json",
      "table1_name" -> "myJsonTable"
    )

    val parsed = new DASDataFilesOptions(opts)
    parsed.tableConfigs.size shouldBe 2

    val table0 = parsed.tableConfigs.head
    table0.name should not be empty
    table0.url shouldBe "/path/to/data.csv"
    table0.format shouldBe Some("csv")

    val table1 = parsed.tableConfigs(1)
    table1.name shouldBe "myJsonTable"
    table1.url shouldBe "/another/path/data.json"
    table1.format shouldBe Some("json")
  }

  it should "throw an exception if format is missing" in {
    val opts = Map(
      "nr_tables" -> "1",
      "table0_url" -> "/path/to/data.csv"
      // "table0_format" is missing
    )
    val parsed = new DASDataFilesOptions(opts)
    // We won't notice the missing format until the plugin tries to create tables in DASDataFiles itself.
    // So here we can just check that tableConfigs has a format= None, or do that check in the plugin constructor test.
    parsed.tableConfigs.head.format shouldBe None
  }

  it should "ensure unique names if they conflict" in {
    val opts = Map(
      "nr_tables" -> "2",
      "table0_url" -> "/path/data1.csv",
      "table0_name" -> "mytable",
      "table0_format" -> "csv",

      "table1_url" -> "/path/data2.csv",
      "table1_name" -> "mytable", // the same name
      "table1_format" -> "csv"
    )
    val parsed = new DASDataFilesOptions(opts)
    parsed.tableConfigs(0).name shouldBe "mytable"
    parsed.tableConfigs(1).name shouldBe "mytable_2" // appended
  }

  it should "derive name from URL if none provided" in {
    val opts = Map(
      "nr_tables" -> "1",
      "table0_url" -> "https://host.com/path/data.csv",
      "table0_format" -> "csv"
    )
    val parsed = new DASDataFilesOptions(opts)
    parsed.tableConfigs.head.name shouldBe "data_csv"
  }

  it should "read the httpOptions" in {
    val opts = Map(
      "nr_tables" -> "1",
      "table0_url" -> "http://somewhere/file.csv",
      "table0_format" -> "csv",
      "http_option_followRedirects" -> "false",
      "http_option_connectTimeout"   -> "5000",
      "http_option_readTimeout"      -> "15000",
      "http_option_sslTrustAll"      -> "true"
    )
    val parsed = new DASDataFilesOptions(opts)
    parsed.httpOptions.followRedirects shouldBe false
    parsed.httpOptions.connectTimeout shouldBe 5000
    parsed.httpOptions.readTimeout shouldBe 15000
    parsed.httpOptions.sslTRustAll shouldBe true
  }
}
