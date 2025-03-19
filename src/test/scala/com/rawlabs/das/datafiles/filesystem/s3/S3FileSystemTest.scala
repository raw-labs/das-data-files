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

package com.rawlabs.das.datafiles.filesystem.s3

import java.io.ByteArrayInputStream

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import com.rawlabs.das.datafiles.filesystem.FileSystemError

import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

class S3FileSystemTest extends AnyFlatSpec with Matchers with MockitoSugar {

  "S3FileSystem.list" should "return a single file if headObject succeeds" in {
    // 1) Mock S3Client
    val mockClient = mock[S3Client]

    // 2) Stub calls
    // If we call headObject, we want it to succeed (no exception).
    when(mockClient.headObject(any[HeadObjectRequest])).thenReturn(
      HeadObjectResponse
        .builder()
        .contentLength(123L)
        .build())

    // 3) Create S3FileSystem with mock client
    val fs = new S3FileSystem(mockClient, "/tmp/test-cache")

    // 4) Call list on a single object
    val url = "s3://mybucket/somefile.csv"
    val result = fs.list(url)

    // 5) Verify
    result shouldBe Right(List(url))
    verify(mockClient, times(1)).headObject(any[HeadObjectRequest])
    verify(mockClient, never()).listObjectsV2(any[ListObjectsV2Request])
  }

  it should "return all objects in a prefix if headObject throws NoSuchKeyException" in {
    val mockClient = mock[S3Client]

    // headObject => throw => we interpret that as "maybe it's a directory"
    when(mockClient.headObject(any[HeadObjectRequest]))
      .thenThrow(NoSuchKeyException.builder().message("No such key").build())

    // Then we call listObjectsV2 => Return a page of results
    val objSummary = S3Object.builder().key("folder/file1.csv").build()
    val resp = ListObjectsV2Response
      .builder()
      .contents(objSummary)
      .build()

    // We want only 1 page here
    when(mockClient.listObjectsV2(any[ListObjectsV2Request]))
      .thenReturn(resp)

    val fs = new S3FileSystem(mockClient, "/tmp/test-cache")

    val url = "s3://mybucket/folder/"
    val result = fs.list(url)

    result.isRight shouldBe true
    val files = result.toOption.get
    files should contain("s3://mybucket/folder/file1.csv")

    verify(mockClient, times(1)).headObject(any[HeadObjectRequest])
    verify(mockClient, atLeastOnce()).listObjectsV2(any[ListObjectsV2Request])
  }

  "S3FileSystem.open" should "return an input stream when getObject is successful" in {
    val mockClient = mock[S3Client]

    // Mock getObject to return some bytes
    val mockContent = "hello s3".getBytes()
    val ris = new ResponseInputStream[GetObjectResponse](
      GetObjectResponse.builder().build(),
      new ByteArrayInputStream(mockContent))

    when(mockClient.getObject(any[GetObjectRequest])).thenReturn(ris)

    val fs = new S3FileSystem(mockClient, "/tmp/test-cache")
    val url = "s3://bucket/file.txt"

    val openResult = fs.open(url)
    openResult.isRight shouldBe true

    val stream = openResult.getOrElse(fail("Expected Right"))
    val readStr = scala.io.Source.fromInputStream(stream).mkString
    readStr shouldBe "hello s3"

    verify(mockClient).getObject(any[GetObjectRequest])
  }

  it should "return NotFound if getObject throws NoSuchKeyException" in {
    val mockClient = mock[S3Client]
    when(mockClient.getObject(any[GetObjectRequest]))
      .thenThrow(NoSuchKeyException.builder().build())

    val fs = new S3FileSystem(mockClient, "/tmp/test-cache")
    val result = fs.open("s3://bucket/missing.txt")

    result shouldBe Left(FileSystemError.NotFound("s3://bucket/missing.txt"))
    verify(mockClient).getObject(any[GetObjectRequest])
  }

  "S3FileSystem.resolveWildcard" should "return only files matching *.csv in the prefix" in {
    val mockClient = mock[S3Client]

    // 1) Because there's a wildcard, S3FileSystem won't call headObject; it calls listObjectsRecursive directly.
    //    But your code first tries headObject to see if it's a single object.
    //    If so, let's just skip that logic by forcing an exception or a 404 on headObject
    //    so that it proceeds to treat it as a directory prefix.
    when(mockClient.headObject(any[HeadObjectRequest]))
      .thenThrow(NoSuchKeyException.builder().message("No such key").build())
    // 2) Mocking listObjectsV2 to return certain keys.
    //    Suppose our prefix is "data/" and we want to see if the user is requesting *.csv.
    //    We'll return some .csv and some .json to confirm only .csv are matched.
    val objectsPage = ListObjectsV2Response
      .builder()
      .contents(
        S3Object.builder().key("data/file1.csv").build(),
        S3Object.builder().key("data/file2.json").build(),
        S3Object.builder().key("data/file3.csv").build())
      .build()

    // For simplicity, only one page of results:
    when(mockClient.listObjectsV2(any[ListObjectsV2Request])).thenReturn(objectsPage)

    // 3) Build S3FileSystem with our mock client
    val fs = new S3FileSystem(mockClient, "/tmp/mock-cache")

    val url = "s3://mybucket/data/*.csv"
    val result = fs.resolveWildcard(url)

    // 4) Assertions
    result.isRight shouldBe true
    val matches = result.toOption.get
    matches should contain theSameElementsAs List("s3://mybucket/data/file1.csv", "s3://mybucket/data/file3.csv")
    // Confirm that file2.json is excluded because the pattern is *.csv

    // 5) Verify that we invoked listObjectsV2.
    verify(mockClient, atLeastOnce()).listObjectsV2(any[ListObjectsV2Request])
  }

  it should "return an empty list if no objects match the wildcard" in {
    val mockClient = mock[S3Client]
    when(mockClient.headObject(any[HeadObjectRequest])).thenThrow(NoSuchKeyException.builder().build())
    // Provide a page of objects, but none match the pattern
    val objectsPage = ListObjectsV2Response
      .builder()
      .contents(S3Object.builder().key("data/fileA.json").build(), S3Object.builder().key("data/fileB.xml").build())
      .build()

    when(mockClient.listObjectsV2(any[ListObjectsV2Request])).thenReturn(objectsPage)

    val fs = new S3FileSystem(mockClient, "/tmp/cache")
    val result = fs.resolveWildcard("s3://mybucket/data/*.csv")
    result.isRight shouldBe true
    result.toOption.get shouldBe empty
  }

  it should "return an error if listing fails for some reason" in {
    val mockClient = mock[S3Client]
    when(mockClient.headObject(any[HeadObjectRequest])).thenThrow(NoSuchKeyException.builder().build())
    // Simulate an unexpected error in listObjectsV2
    when(mockClient.listObjectsV2(any[ListObjectsV2Request]))
      .thenThrow(new RuntimeException("some internal error"))

    val fs = new S3FileSystem(mockClient, "/tmp/cache")
    val result = fs.resolveWildcard("s3://mybucket/data/*.csv")

    result.isLeft shouldBe true
    result.swap.getOrElse(fail("expected left")) shouldBe a[FileSystemError.GenericError]
  }
}
