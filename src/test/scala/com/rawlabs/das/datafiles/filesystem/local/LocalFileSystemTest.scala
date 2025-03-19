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

package com.rawlabs.das.datafiles.filesystem.local

import java.io.File
import java.nio.file.Files

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LocalFileSystemTest extends AnyFlatSpec with Matchers {
  // Suppose you create a temporary directory with multiple files for test
  // Then call localFs.resolveWildcard("file:///someTempDir/*.csv") and verify results.

  it should "resolve wildcard patterns in local directories" in {
    val tempDir = Files.createTempDirectory("local-wildcard-test").toFile
    // create file1.csv, file2.json inside it
    val csv1 = new File(tempDir, "file1.csv")
    val json1 = new File(tempDir, "file2.json")
    Files.write(csv1.toPath, "some,csv,content\n".getBytes)
    Files.write(json1.toPath, """{"foo":123}""".getBytes)

    val fs = new LocalFileSystem("/tmp/cache", maxDownloadSize = 1000000L)
    val result = fs.resolveWildcard(tempDir.toURI.toString + "/*.csv")

    result.isRight shouldBe true
    val normalized = result.toOption.get.map(normalizeUrl)
    normalized should contain(normalizeUrl(csv1.toURI.toString))
    normalized should not contain normalizeUrl(json1.toURI.toString)
  }

  private def normalizeUrl(url: String): String = {
    url.replace("file:///", "file:/")
  }
}
