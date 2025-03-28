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
import scala.util.control.NonFatal

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
      val headReq = HeadObjectRequest.builder().bucket(bucket).key(key).build()
      s3Client.headObject(headReq)

      // If HEAD is successful => it's a single file
      Right(List(url))
    } catch {
      case _: NoSuchKeyException =>
        // Not a single object => treat as a prefix (like "directory") but only depth=1
        listAsDirectorySingleLevel(bucket, key)
      case e: NoSuchBucketException =>
        Left(FileSystemError.NotFound(url, s"Bucket not found  => ${e.getMessage}"))
      case e: LimitExceededException =>
        Left(FileSystemError.TooManyRequests(s"Too many requests $url => ${e.getMessage}"))
      case e: TooManyRequestsException =>
        Left(FileSystemError.TooManyRequests(s"Too many requests $url => ${e.getMessage}"))
      case e: S3Exception if e.statusCode() == 403 =>
        Left(FileSystemError.PermissionDenied(s"Forbidden $url => ${e.getMessage}"))
      case e: S3Exception if e.statusCode() == 401 =>
        Left(FileSystemError.Unauthorized(s"Unauthorized $url => ${e.getMessage}"))
      case _: AccessDeniedException =>
        Left(FileSystemError.PermissionDenied(s"Access denied listing $url"))
      case NonFatal(e) =>
        logger.error(s"Error listing s3 url $url", e)
        throw e
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
      case e: NoSuchBucketException =>
        Left(FileSystemError.NotFound(url, s"Bucket not found  => ${e.getMessage}"))
      case e: NoSuchKeyException =>
        Left(FileSystemError.NotFound(url, s"key not found => ${e.getMessage}"))
      case e: LimitExceededException =>
        Left(FileSystemError.TooManyRequests(s"Too many requests $url => ${e.getMessage}"))
      case e: TooManyRequestsException =>
        Left(FileSystemError.TooManyRequests(s"Too many requests $url => ${e.getMessage}"))
      case e: S3Exception if e.statusCode() == 403 =>
        Left(FileSystemError.PermissionDenied(s"Forbidden $url => ${e.getMessage}"))
      case e: S3Exception if e.statusCode() == 401 =>
        Left(FileSystemError.Unauthorized(s"Unauthorized $url => ${e.getMessage}"))
      case _: AccessDeniedException =>
        Left(FileSystemError.PermissionDenied(s"Access denied listing $url"))
      case NonFatal(e) =>
        logger.error(s"Error opening s3 url $url", e)
        throw e
    }
  }

  /**
   * Resolves wildcard patterns in `url`, e.g. "s3://bucket/path/\*.csv". We do a naive approach: list the entire
   * single-level prefix and filter by pattern.
   */
  override def resolveWildcard(url: String): Either[FileSystemError, List[String]] = {

    try {
      val (bucket, fullPath) = parseS3Url(url) match {
        case Left(error)   => return Left(error)
        case Right(values) => values
      }

      val (prefix, maybePattern) = splitWildcard(fullPath)
      if (maybePattern.isEmpty) return list(url)

      val regex = ("^" + prefix + globToRegex(maybePattern.get) + "$").r
      // Single-level listing for the prefix
      val objects = listObjectsSingleLevel(bucket, prefix)
      val matched = objects.filter(regex.matches)

      Right(matched.map(k => s"s3://$bucket/$k"))
    } catch {
      case e: NoSuchBucketException =>
        Left(FileSystemError.NotFound(url, s"Bucket not found  => ${e.getMessage}"))
      case e: NoSuchKeyException =>
        Left(FileSystemError.NotFound(url, s"key not found => ${e.getMessage}"))
      case e: LimitExceededException =>
        Left(FileSystemError.TooManyRequests(s"Too many requests $url => ${e.getMessage}"))
      case e: TooManyRequestsException =>
        Left(FileSystemError.TooManyRequests(s"Too many requests $url => ${e.getMessage}"))
      case e: S3Exception if e.statusCode() == 403 =>
        Left(FileSystemError.PermissionDenied(s"Forbidden $url => ${e.getMessage}"))
      case e: S3Exception if e.statusCode() == 401 =>
        Left(FileSystemError.Unauthorized(s"Unauthorized $url => ${e.getMessage}"))
      case _: AccessDeniedException =>
        Left(FileSystemError.PermissionDenied(s"Access denied listing $url"))
      case NonFatal(e) =>
        logger.error(s"Error resolving s3 wildcard $url", e)
        throw e
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
      case e: NoSuchBucketException =>
        Left(FileSystemError.NotFound(url, s"Bucket not found  => ${e.getMessage}"))
      case e: NoSuchKeyException =>
        Left(FileSystemError.NotFound(url, s"key not found => ${e.getMessage}"))
      case e: LimitExceededException =>
        Left(FileSystemError.TooManyRequests(s"Too many requests $url => ${e.getMessage}"))
      case e: TooManyRequestsException =>
        Left(FileSystemError.TooManyRequests(s"Too many requests $url => ${e.getMessage}"))
      case e: S3Exception if e.statusCode() == 403 =>
        Left(FileSystemError.PermissionDenied(s"Forbidden $url => ${e.getMessage}"))
      case e: S3Exception if e.statusCode() == 401 =>
        Left(FileSystemError.Unauthorized(s"Unauthorized $url => ${e.getMessage}"))
      case _: AccessDeniedException =>
        Left(FileSystemError.PermissionDenied(s"Access denied listing $url"))
      case NonFatal(e) =>
        logger.error(s"Error getting s3 url size $url", e)
        throw e
    }
  }

  override def stop(): Unit = {
    s3Client.close()
  }

  // --------------------------------------------------------------------------
  // Private Helpers
  // --------------------------------------------------------------------------

  /**
   * If HEAD fails, we treat the path as a "folder" prefix and do a single-level listing. Returns Right(list-of-S3-URIs)
   * or NotFound if empty.
   */
  private def listAsDirectorySingleLevel(bucket: String, prefix: String): Either[FileSystemError, List[String]] = {
    val objectKeys = listObjectsSingleLevel(bucket, prefix)

    val uris = objectKeys.map(k => s"s3://$bucket/$k")
    Right(uris)

  }

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
    options.get("aws_region").foreach(r => builder.region(Region.of(r)))

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
