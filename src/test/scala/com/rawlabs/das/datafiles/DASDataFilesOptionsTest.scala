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

import com.rawlabs.das.datafiles.utils.DASDataFilesOptions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DASDataFilesOptionsTest extends AnyFlatSpec with Matchers {

  behavior of "DASDataFilesOptions"

  it should "parse multiple paths" in {
    val opts = Map(
      "paths" -> "2",

      // Table0
      "path0_url" -> "/path/to/data.csv",
      "path0_format" -> "csv",

      // path1
      "path1_url" -> "/another/path/data.json",
      "path1_format" -> "json",
      "path1_name" -> "myJsonTable")

    val parsed = new DASDataFilesOptions(opts)
    parsed.pathConfig.size shouldBe 2

    val path0 = parsed.pathConfig.head
    path0.uri.toString shouldBe "/path/to/data.csv"
    path0.format shouldBe Some("csv")

    val path1 = parsed.pathConfig(1)
    path1.maybeName shouldBe Some("myJsonTable")
    path1.uri.toString shouldBe "/another/path/data.json"
    path1.format shouldBe Some("json")
  }

}
