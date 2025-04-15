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

package com.rawlabs.das.datafiles.filesystem.local

import java.io.File
import java.nio.file.Files

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.datafiles.filesystem.FileSystemError

class LocalFileSystemTest extends AnyFlatSpec with Matchers {

  // Helper to normalize file URIs (to account for differences in file URL formats)
  private def normalizeUrl(url: String): String = {
    url.replace("file:///", "file:/")
  }

  it should "resolve wildcard patterns in local directories" in {
    val tempDir = Files.createTempDirectory("local-wildcard-test").toFile
    // Create file1.csv and file2.json
    val csv1 = new File(tempDir, "file1.csv")
    val json1 = new File(tempDir, "file2.json")
    Files.write(csv1.toPath, "some,csv,content\n".getBytes)
    Files.write(json1.toPath, """{"foo":123}""".getBytes)

    val fs = new LocalFileSystem()
    val result = fs.resolveWildcard(tempDir.toURI.toString + "/*.csv")

    result.isRight shouldBe true
    val normalized = result.toOption.get.map(normalizeUrl)
    normalized should contain(normalizeUrl(csv1.toURI.toString))
    normalized should not contain normalizeUrl(json1.toURI.toString)
  }

  it should "list files in a directory" in {
    val tempDir = Files.createTempDirectory("local-list-test").toFile
    // Create two files and one subdirectory
    val file1 = new File(tempDir, "a.txt")
    val file2 = new File(tempDir, "b.txt")
    val subDir = new File(tempDir, "subdir")
    subDir.mkdir()
    Files.write(file1.toPath, "content".getBytes)
    Files.write(file2.toPath, "content".getBytes)

    val fs = new LocalFileSystem()
    val result = fs.list(tempDir.toURI.toString)

    result.isRight shouldBe true
    val files = result.toOption.get.map(normalizeUrl)
    files should contain(normalizeUrl(file1.toURI.toString))
    files should contain(normalizeUrl(file2.toURI.toString))
    // Directories should not be listed.
    files should not contain normalizeUrl(subDir.toURI.toString)
  }

  it should "list a single file when URL is a file" in {
    val tempFile = Files.createTempFile("local-single-test", ".txt").toFile
    Files.write(tempFile.toPath, "hello".getBytes)

    val fs = new LocalFileSystem()
    val result = fs.list(tempFile.toURI.toString)

    result.isRight shouldBe true
    result.toOption.get.map(normalizeUrl) should contain(normalizeUrl(tempFile.toURI.toString))
  }

  it should "return NotFound error when listing a non-existent file" in {
    val nonExistent = new File("non_existent_file.txt")
    val fs = new LocalFileSystem()
    val result = fs.list(nonExistent.toURI.toString)

    result.swap.getOrElse(fail("expected left")) shouldBe a[FileSystemError.NotFound]
  }

  it should "open a file and read its content" in {
    val tempFile = Files.createTempFile("local-open-test", ".txt").toFile
    val content = "Hello, world!"
    Files.write(tempFile.toPath, content.getBytes)

    val fs = new LocalFileSystem()
    val result = fs.open(tempFile.toURI.toString)

    result.isRight shouldBe true
    val stream = result.toOption.get
    val readContent = scala.io.Source.fromInputStream(stream).mkString
    stream.close()
    readContent shouldEqual content
  }

  it should "return error when opening a directory" in {
    val tempDir = Files.createTempDirectory("local-open-dir-test").toFile
    val fs = new LocalFileSystem()
    val result = fs.open(tempDir.toURI.toString)

    result.swap.getOrElse(fail("expected left")) shouldBe a[FileSystemError.Unsupported]
  }

  it should "return NotFound error when opening a non-existent file" in {
    val nonExistent = new File("non_existent_file.txt")
    val fs = new LocalFileSystem()
    val result = fs.open(nonExistent.toURI.toString)

    result.swap.getOrElse(fail("expected left")) shouldBe a[FileSystemError.NotFound]
  }

  it should "resolve wildcard patterns with multiple matches" in {
    val tempDir = Files.createTempDirectory("local-wildcard-multi-test").toFile
    val file1 = new File(tempDir, "data1.txt")
    val file2 = new File(tempDir, "data2.txt")
    val file3 = new File(tempDir, "image.png")
    Files.write(file1.toPath, "text".getBytes)
    Files.write(file2.toPath, "text".getBytes)
    Files.write(file3.toPath, "image".getBytes)

    val fs = new LocalFileSystem()
    val result = fs.resolveWildcard(tempDir.toURI.toString + "/*.txt")

    result.isRight shouldBe true
    val files = result.toOption.get.map(normalizeUrl)
    files should contain(normalizeUrl(file1.toURI.toString))
    files should contain(normalizeUrl(file2.toURI.toString))
    files should not contain normalizeUrl(file3.toURI.toString)
  }

  it should "resolve wildcard to empty list when no files match" in {
    val tempDir = Files.createTempDirectory("local-wildcard-empty-test").toFile
    // Create a file that doesn't match the pattern
    val file1 = new File(tempDir, "file1.txt")
    Files.write(file1.toPath, "content".getBytes)

    val fs = new LocalFileSystem()
    val result = fs.resolveWildcard(tempDir.toURI.toString + "/*.csv")

    result.isRight shouldBe true
    result.toOption.get shouldBe empty
  }

  it should "get file size for a file" in {
    val tempFile = Files.createTempFile("local-size-test", ".txt").toFile
    val content = "SizeTest"
    Files.write(tempFile.toPath, content.getBytes)

    val fs = new LocalFileSystem()
    val result = fs.getFileSize(tempFile.toURI.toString)

    result.isRight shouldBe true
    result.toOption.get shouldEqual tempFile.length()
  }

  it should "return error when getting file size on a directory" in {
    val tempDir = Files.createTempDirectory("local-size-dir-test").toFile
    val fs = new LocalFileSystem()
    val result = fs.getFileSize(tempDir.toURI.toString)

    result.swap.getOrElse(fail("expected left")) shouldBe a[FileSystemError.Unsupported]
  }

  it should "support file URLs" in {
    val fs = new LocalFileSystem()
    fs.supportsUrl("file:///tmp/somefile.txt") shouldBe true
    fs.supportsUrl("/tmp/somefile.txt") shouldBe true
  }

  it should "not support non-file URLs" in {
    val fs = new LocalFileSystem()
    fs.supportsUrl("http://example.com/file.txt") shouldBe false
  }

  it should "return InvalidUrl error for malformed URLs" in {
    val fs = new LocalFileSystem()
    val result = fs.list("::invalid-url::")
    result.swap.getOrElse(fail("expected left")) shouldBe a[FileSystemError.InvalidUrl]
  }

  it should "stop without errors" in {
    val fs = new LocalFileSystem()
    noException should be thrownBy fs.stop()
  }

  it should "return PermissionDenied error when opening a file with restricted permissions" in {
    val tempFile = Files.createTempFile("local-permission-test", ".txt").toFile
    Files.write(tempFile.toPath, "secret".getBytes)
    // Attempt to simulate a permission error by removing read permissions.
    tempFile.setReadable(false, false)

    val fs = new LocalFileSystem()
    val result = fs.open(tempFile.toURI.toString)
    result.swap.getOrElse(fail("expected left")) shouldBe a[FileSystemError.PermissionDenied]

    // Reset permissions for cleanup.
    tempFile.setReadable(true, false)
  }
}
