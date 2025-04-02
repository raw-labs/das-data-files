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

package com.rawlabs.das.datafiles.filesystem.s3

import java.io.{BufferedInputStream, InputStream}
import java.net.URI

import scala.jdk.CollectionConverters._

import com.rawlabs.das.datafiles.filesystem.FileSystemError
import com.rawlabs.das.datafiles.filesystem.api.BaseFileSystem
import com.rawlabs.das.sdk.DASSdkInvalidArgumentException

import software.amazon.awssdk.auth.credentials.{
  AnonymousCredentialsProvider,
  AwsBasicCredentials,
  StaticCredentialsProvider
}
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.account.model.{AccessDeniedException, TooManyRequestsException}
import software.amazon.awssdk.services.acm.model.LimitExceededException
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

/**
 * S3FileSystem that uses the AWS SDK v2 for S3 operations (list, open, wildcard resolution, etc.).
 */
class S3FileSystem(s3Client: S3Client, cacheFolder: String, maxDownloadSize: Long = 100 * 1024 * 1024)
    extends BaseFileSystem(cacheFolder, maxDownloadSize) {

  // --------------------------------------------------------------------------
  // Public API
  // --------------------------------------------------------------------------

  val name = "s3"

  override def supportsUrl(url: String): Boolean = parseS3Url(url).isRight

  /**
   * Lists files at the given S3 `url`, returning either:
   *   - A single object (if HEAD succeeds).
   *   - The contents of a "directory" at depth=1 (if HEAD fails with NoSuchKey).
   */
  override def list(url: String): Either[FileSystemError, List[String]] = {
    val (bucket, key) = parseS3Url(url) match {
      case Left(error)   => return Left(error)
      case Right(values) => values
    }

    try {
      // Try HEAD to see if it's a single object
      try {
        val headReq = HeadObjectRequest.builder().bucket(bucket).key(key).build()
        s3Client.headObject(headReq)
        // If HEAD is successful => it's a single file
        Right(List(url))
      } catch {
        case _: NoSuchKeyException =>
          // If HEAD fails with NoSuchKey, we assume it's a directory and list its contents
          val objectKeys = listObjectsSingleLevel(bucket, key)
          val uris = objectKeys.map(k => s"s3://$bucket/$k")
          Right(uris)
      }
    } catch {
      case _: NoSuchBucketException =>
        Left(FileSystemError.NotFound(url, s"Bucket $bucket not: $url"))
      case _: NoSuchKeyException =>
        Left(FileSystemError.NotFound(url, s"Path $key not found: $url"))
      case _: LimitExceededException | _: TooManyRequestsException =>
        Left(FileSystemError.TooManyRequests(s"Too many requests: $url"))
      case e: S3Exception if e.statusCode() == 403 =>
        Left(FileSystemError.PermissionDenied(s"Forbidden: $url"))
      case e: S3Exception if e.statusCode() == 401 =>
        Left(FileSystemError.Unauthorized(s"Unauthorized: $url "))
      case _: AccessDeniedException =>
        Left(FileSystemError.PermissionDenied(s"Access denied: $url"))
    }
  }

  /**
   * Opens the file at `url` and returns an InputStream if found.
   */
  override def open(url: String): Either[FileSystemError, InputStream] = {
    val (bucket, key) = parseS3Url(url) match {
      case Left(error)   => return Left(error)
      case Right(values) => values
    }

    val getReq = GetObjectRequest.builder().bucket(bucket).key(key).build()

    try {
      val response: ResponseInputStream[_] = s3Client.getObject(getReq)
      Right(new BufferedInputStream(response))
    } catch {
      case _: NoSuchBucketException =>
        Left(FileSystemError.NotFound(url, s"Bucket $bucket not: $url"))
      case _: NoSuchKeyException =>
        Left(FileSystemError.NotFound(url, s"Path $key not found: $url"))
      case _: LimitExceededException | _: TooManyRequestsException =>
        Left(FileSystemError.TooManyRequests(s"Too many requests: $url"))
      case e: S3Exception if e.statusCode() == 403 =>
        Left(FileSystemError.PermissionDenied(s"Forbidden: $url"))
      case e: S3Exception if e.statusCode() == 401 =>
        Left(FileSystemError.Unauthorized(s"Unauthorized: $url "))
      case _: AccessDeniedException =>
        Left(FileSystemError.PermissionDenied(s"Access denied: $url"))
    }
  }

  /**
   * Resolves wildcard patterns in `url`, e.g. "s3://bucket/path/\*.csv". We do a naive approach: list the entire
   * single-level prefix and filter by pattern.
   */
  override def resolveWildcard(url: String): Either[FileSystemError, List[String]] = {

    val (bucket, fullPath) = parseS3Url(url) match {
      case Left(error)   => return Left(error)
      case Right(values) => values
    }

    val (prefix, maybePattern) = splitWildcard(fullPath)
    if (maybePattern.isEmpty) return Right(List(url))

    try {
      val regex = ("^" + prefix + globToRegex(maybePattern.get) + "$").r
      // Single-level listing for the prefix
      val objects = listObjectsSingleLevel(bucket, prefix)
      val matched = objects.filter(regex.matches)
      Right(matched.map(k => s"s3://$bucket/$k"))

    } catch {
      case _: NoSuchBucketException =>
        Left(FileSystemError.NotFound(url, s"Got bucket not found while trying to resolve wildcard $url"))
      case _: NoSuchKeyException =>
        Left(FileSystemError.NotFound(url, s"Got path not found while trying to resolve wildcard $url"))
      case _: LimitExceededException | _: TooManyRequestsException =>
        Left(FileSystemError.TooManyRequests(s"Too many requests while trying to resolve wildcard $url"))
      case e: S3Exception if e.statusCode() == 403 =>
        Left(FileSystemError.PermissionDenied(s"Got forbidden while trying to resolve wildcard $url"))
      case e: S3Exception if e.statusCode() == 401 =>
        Left(FileSystemError.Unauthorized(s"Got unauthorized while trying to resolve wildcard $url"))
    }
  }

  /**
   * Return the size of the S3 file (in bytes), or an error if not found.
   */
  override def getFileSize(url: String): Either[FileSystemError, Long] = {
    val (bucket, key) = parseS3Url(url) match {
      case Left(error)   => return Left(error)
      case Right(values) => values
    }

    try {
      val headReq = HeadObjectRequest.builder().bucket(bucket).key(key).build()
      val headResp = s3Client.headObject(headReq)
      Right(headResp.contentLength())
    } catch {
      case _: NoSuchBucketException =>
        Left(FileSystemError.NotFound(url, s"Bucket $bucket not: $url"))
      case _: NoSuchKeyException =>
        Left(FileSystemError.NotFound(url, s"Path $key not found: $url"))
      case _: LimitExceededException | _: TooManyRequestsException =>
        Left(FileSystemError.TooManyRequests(s"Too many requests: $url"))
      case e: S3Exception if e.statusCode() == 403 =>
        Left(FileSystemError.PermissionDenied(s"Forbidden: $url"))
      case e: S3Exception if e.statusCode() == 401 =>
        Left(FileSystemError.Unauthorized(s"Unauthorized: $url "))
      case _: AccessDeniedException =>
        Left(FileSystemError.PermissionDenied(s"Access denied: $url"))
    }
  }

  override def stop(): Unit = {
    s3Client.close()
  }

  // --------------------------------------------------------------------------
  // Private Helpers
  // --------------------------------------------------------------------------

  /**
   * **Single-level** listing of objects under `prefix`, ignoring deeper subdirectories. We use `delimiter("/")` so that
   * S3 will return only the objects in this folder and treat subfolders as common prefixes (which we omit).
   */
  private def listObjectsSingleLevel(bucket: String, prefix: String): List[String] = {
    var results = List.empty[String]
    var continuationToken: Option[String] = None

    do {
      val reqBuilder = ListObjectsV2Request
        .builder()
        .bucket(bucket)
        .prefix(prefix)
        .delimiter("/")

      continuationToken.foreach(reqBuilder.continuationToken)

      val req = reqBuilder.build()
      val resp: ListObjectsV2Response = s3Client.listObjectsV2(req)

      // Collect immediate objects (not subfolders)
      val pageKeys = resp.contents().asScala.map(_.key()).toList
      results = results ++ pageKeys

      // Move to next page if needed
      continuationToken = Option(resp.nextContinuationToken()).filter(_.nonEmpty)
    } while (continuationToken.isDefined)

    results
  }

  /**
   * Parse "s3://bucket/key" into (bucket, key).
   */
  private def parseS3Url(url: String): Either[FileSystemError, (String, String)] = {
    val uri = new URI(url)

    if (uri.getScheme != "s3") {
      return Left(FileSystemError.Unsupported(s"Unsupported URL scheme: $url"))
    }

    val bucket = uri.getHost
    if (bucket == null) {
      return Left(FileSystemError.InvalidUrl(url, s"Invalid S3 URL, cannot parse bucket"))
    }
    val key = uri.getPath.stripPrefix("/")
    Right((bucket, key))
  }
}

object S3FileSystem {
  def build(options: Map[String, String], cacheFolder: String, maxDownloadSize: Long): S3FileSystem = {
    val builder = S3Client.builder()
    options.get("aws_region").foreach(region => builder.region(Region.of(region)))

    // Credentials
    val credentials =
      if (options.contains("aws_access_key")) {
        val creds = AwsBasicCredentials.create(
          options("aws_access_key"),
          options.getOrElse("aws_secret_key", throw new DASSdkInvalidArgumentException("Missing AWS secret key")))
        StaticCredentialsProvider.create(creds)
      } else {
        AnonymousCredentialsProvider.create()
      }

    builder.credentialsProvider(credentials)

    new S3FileSystem(builder.build(), cacheFolder, maxDownloadSize)
  }
}
