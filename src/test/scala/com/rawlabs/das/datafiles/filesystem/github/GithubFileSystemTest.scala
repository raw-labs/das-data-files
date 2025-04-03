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

package com.rawlabs.das.datafiles.filesystem.github

import java.io.File

import org.apache.commons.io.FileUtils
import org.kohsuke.github._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import com.rawlabs.das.datafiles.filesystem.FileSystemError

class GithubFileSystemTest extends AnyFlatSpec with Matchers with MockitoSugar {

  "GithubFileSystem.list" should "list a directory of files" in {
    // 1) Create mocks
    val mockGitHub = mock[GitHub]
    val mockRepo = mock[GHRepository]
    val mockContentFile = mock[GHContent]
    val mockContentDir = mock[GHContent]

    // 2) Stubbing: getRepository => mockRepo
    when(mockGitHub.getRepository("owner/repo")).thenReturn(mockRepo)

    // 3) The directory listing:
    // "repo.getDirectoryContent(path, branch)" => List(GHContent)
    val listOfContents = java.util.Arrays.asList(mockContentFile)
    when(mockRepo.getDirectoryContent("folder", "main")).thenReturn(listOfContents)
    when(mockRepo.getFileContent("folder", "main")).thenReturn(mockContentDir)
    when(mockContentDir.isFile).thenReturn(false)
    when(mockContentDir.getPath).thenReturn("folder")

    // The mockContentFile isFile => true, getPath => "folder/file.csv"
    when(mockContentFile.isFile).thenReturn(true)
    when(mockContentFile.getPath).thenReturn("folder/file.csv")

    // 4) Instantiate GithubFileSystem with the mock
    val fs = new GithubFileSystem(mockGitHub)

    // 5) Call list
    val url = "github://owner/repo/main/folder"
    val result = fs.list(url)

    // 6) Verify
    result.isRight shouldBe true
    val files = result.toOption.get
    files shouldBe List("github://owner/repo/main/folder/file.csv")

    verify(mockGitHub).getRepository("owner/repo")
    verify(mockRepo).getDirectoryContent("folder", "main")
  }

  it should "return an NotFound error if getRepository throws an GHFileNotFoundException" in {
    val mockGitHub = mock[GitHub]
    when(mockGitHub.getRepository("owner/repo")).thenThrow(new GHFileNotFoundException("repo not found"))

    val fs = new GithubFileSystem(mockGitHub)

    val url = "github://owner/repo/main/doesnotmatter"
    val result = fs.list(url)
    result.isLeft shouldBe true
    val leftVal = result.swap.getOrElse(fail("Expected a left value"))
    leftVal shouldBe a[FileSystemError.NotFound]
  }

  "GithubFileSystem.open" should "retrieve a file and return an InputStream" in {
    val mockGitHub = mock[GitHub]
    val mockRepo = mock[GHRepository]
    val mockFile = mock[GHContent]

    val tempFile: File = {
      val f = File.createTempFile("fake-github-file", ".csv")
      f.deleteOnExit()
      FileUtils.writeStringToFile(f, "a,b\n1,2\n3,4", "UTF-8")
      f
    }
    when(mockGitHub.getRepository("owner/repo")).thenReturn(mockRepo)
    when(mockRepo.getFileContent("folder/data.csv", "main")).thenReturn(mockFile)

    // The GHContent has a download URL, e.g. https://raw.githubusercontent.com/...
    // We'll mock as local temp file
    when(mockFile.getDownloadUrl).thenReturn("file:" + tempFile.getAbsolutePath)

    val fs = new GithubFileSystem(mockGitHub)
    val url = "github://owner/repo/main/folder/data.csv"
    val openResult = fs.open(url)
    openResult.isRight shouldBe true
    // This will try to open a "fake" URL. If your network call tries to connect, it might fail.
    // For a purely offline test, you'd need a higher-level approach (like a custom HTTP client you can mock).

    verify(mockRepo).getFileContent("folder/data.csv", "main")
  }

  "GithubFileSystem.resolveWildcard" should "filter files to only match *.csv" in {
    val mockGitHub = mock[GitHub]
    val mockRepo = mock[GHRepository]
    val csvFile = mock[GHContent]
    val jsonFile = mock[GHContent]

    when(mockGitHub.getRepository("owner/repo")).thenReturn(mockRepo)

    // Step 1: The code's "resolveWildcard" => splits "github://owner/repo/main/folder/*.csv"
    // into prefixUrl = "github://owner/repo/main/folder" and pattern = "*.csv".
    // Then it calls "list(prefixUrl)" => which calls "getDirectoryContent("folder", "main")"

    // So let's mock "getDirectoryContent".
    val contents = java.util.Arrays.asList(csvFile, jsonFile)
    when(mockRepo.getDirectoryContent("folder", "main")).thenReturn(contents)

    // Mark the first as a file with path "folder/data1.csv"
    when(csvFile.isFile).thenReturn(true)
    when(csvFile.getPath).thenReturn("folder/data1.csv")

    // Mark the second as a file with path "folder/data2.json"
    when(jsonFile.isFile).thenReturn(true)
    when(jsonFile.getPath).thenReturn("folder/data2.json")

    val fs = new GithubFileSystem(mockGitHub)

    // Step 2: Actually call resolveWildcard
    val wildcardUrl = "github://owner/repo/main/folder/*.csv"
    val result = fs.resolveWildcard(wildcardUrl)

    result.isRight shouldBe true
    val matchedFiles = result.toOption.get

    // We expect only the CSV file to come back
    matchedFiles should contain theSameElementsAs Seq("github://owner/repo/main/folder/data1.csv")
    matchedFiles should not contain "github://owner/repo/main/folder/data2.json"

    // Step 3: Verify interactions
    verify(mockGitHub).getRepository("owner/repo")
    verify(mockRepo).getDirectoryContent("folder", "main")
  }

  it should "fall back return url if there's no wildcard in the URL and it is a file" in {
    val mockGitHub = mock[GitHub]
    val mockRepo = mock[GHRepository]
    when(mockGitHub.getRepository("owner/repo")).thenReturn(mockRepo)

    // If there's no wildcard, code calls "list(url)"
    val fileObj = mock[GHContent]
    when(mockRepo.getFileContent("file.csv", "main")).thenReturn(fileObj)
    when(fileObj.isFile).thenReturn(true)
    when(fileObj.getPath).thenReturn("file.csv")

    val fs = new GithubFileSystem(mockGitHub)
    val url = "github://owner/repo/main/file.csv" // no wildcard
    val result = fs.resolveWildcard(url)

    // We expect it to do the normal "list" => we get data.csv
    result.isRight shouldBe true
    val files = result.toOption.get
    files should contain("github://owner/repo/main/file.csv")
  }

  it should "return permission denied if HttpException id thrown with 403" in {
    val mockGitHub = mock[GitHub]
    when(mockGitHub.getRepository("owner/repo"))
      .thenThrow(new HttpException("Some GitHub error", 403, "Forbidden", "https://github.com"))

    val fs = new GithubFileSystem(mockGitHub)
    val result = fs.resolveWildcard("github://owner/repo/main/folder/*.csv")

    result.isLeft shouldBe true
    result.swap.getOrElse(fail("expected lef")) shouldBe a[FileSystemError.PermissionDenied]
  }

}
