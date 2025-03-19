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

import com.rawlabs.das.datafiles.filesystem.{DASFileSystem, FileSystemError}
import com.rawlabs.das.sdk.DASSdkInvalidArgumentException

import java.io.{BufferedInputStream, InputStream}
import java.net.URI
import scala.util.control.NonFatal

// AWS SDK v2 imports
import software.amazon.awssdk.auth.credentials.{AnonymousCredentialsProvider, AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

/**
 * S3FileSystem that uses the AWS SDK v2 for S3 operations (list, open, wildcard resolution, etc.).
 *
 * @param accessKey optional AWS access key
 * @param secretKey optional AWS secret key
 * @param region optional region (e.g. "us-east-1")
 * @param cacheFolder local folder for caching/downloading, if needed
 */
class S3FileSystem(accessKey: Option[String], secretKey: Option[String], region: Option[String], cacheFolder: String)
    extends DASFileSystem(cacheFolder) {

  // --------------------------------------------------------------------------
  // Build the S3Client using the AWS SDK v2
  // --------------------------------------------------------------------------

  private val s3Client: S3Client = {
    val builder = S3Client.builder()

    // Region
    region.foreach(r => builder.region(Region.of(r)))

    // You can also customize the S3 config, like disabling chunked encoding, path style, etc.
    // builder.serviceConfiguration(S3Configuration.builder().build())

    // Credentials
    // 1) If user specified keys, use them
    // 2) Otherwise, use anonymous (or DefaultCredentialsProvider, if you prefer)
    val credentials =
      if (accessKey.isDefined) {
        val creds = AwsBasicCredentials.create(
          accessKey.get,
          secretKey.getOrElse(throw new DASSdkInvalidArgumentException("Missing AWS secret key")))
        StaticCredentialsProvider.create(creds)
      } else {
        // Consider using DefaultCredentialsProvider.create() if you want to pick up environment or profile
        AnonymousCredentialsProvider.create()
      }

    builder.credentialsProvider(credentials)
    builder.build()
  }

  // --------------------------------------------------------------------------
  // Public API
  // --------------------------------------------------------------------------

  /**
   * Lists files at the given S3 `url`, returning either a single object or the contents of a "directory."
   *
   * Example: list("s3://my-bucket/path/prefix/") => Right(List of child objects) If the url is a single object (no
   * trailing slash, presumably) and it exists => one-element list
   */
  override def list(url: String): Either[FileSystemError, List[String]] = {
    // Ensure it starts with s3:// or s3a://
    if (!url.startsWith("s3://") ) {
      return Left(FileSystemError.Unsupported(s"Unsupported S3 URL: $url"))
    }

    val (bucket, key) = parseS3Url(url)

    try {
      // 1) Try HEAD to see if it's a single object
      val headReq = HeadObjectRequest.builder().bucket(bucket).key(key).build()
      s3Client.headObject(headReq)
      // If HEAD is successful => it's a single file
      Right(List(s"$url")) // We can return the exact original URI or an "s3a://" variant if you prefer
    } catch {
      case e: NoSuchKeyException =>
        // Object not found => maybe it's a prefix (like a directory)
        listAsDirectory(bucket, key, url)
      case e: S3Exception if e.statusCode() == 404 =>
        // Also treat 404 => maybe a prefix
        listAsDirectory(bucket, key, url)
      case NonFatal(e) =>
        Left(FileSystemError.GenericError(s"Error listing $url => ${e.getMessage}"))
    }
  }

  /**
   * Opens the file at `url` and returns an InputStream if found.
   */
  override def open(url: String): Either[FileSystemError, InputStream] = {
    if (!url.startsWith("s3://")) {
      return Left(FileSystemError.Unsupported(s"Unsupported S3 URL: $url"))
    }

    val (bucket, key) = parseS3Url(url)
    val getReq = GetObjectRequest.builder().bucket(bucket).key(key).build()

    try {
      val response: ResponseInputStream[_] = s3Client.getObject(getReq)
      Right(new BufferedInputStream(response))
    } catch {
      case e: NoSuchKeyException =>
        Left(FileSystemError.NotFound(url))
      case e: S3Exception if e.statusCode() == 403 =>
        Left(FileSystemError.PermissionDenied(s"Forbidden to open $url => ${e.getMessage}"))
      case e: S3Exception if e.statusCode() == 401 =>
        Left(FileSystemError.Unauthorized(s"Unauthorized to open $url => ${e.getMessage}"))
      case e: S3Exception if e.statusCode() == 404 =>
        Left(FileSystemError.NotFound(url))
      case NonFatal(e) =>
        Left(FileSystemError.GenericError(s"Error opening S3 file: $url => ${e.getMessage}"))
    }
  }

  /**
   * Resolves wildcard patterns in `url`, e.g. "s3://bucket/path/\*.csv" We'll do a naive approach:
   *   - Split prefix (everything up to first wildcard) vs. suffix
   *   - list the entire prefix
   *   - filter the results
   */
  override def resolveWildcard(url: String): Either[FileSystemError, List[String]] = {

    // 1) parse bucket + full path
    val (bucket, fullPath) = parseS3Url(url)
    val wildcardIndex = fullPath.indexWhere(c => c == '*' || c == '?')

    if (wildcardIndex < 0) {
      // No actual wildcard => just do normal listing
      return list(url)
    }

    val prefix = fullPath.substring(0, wildcardIndex)
    // naive approach to get directory prefix (some users do "prefix/???.csv" etc.)
    val prefixOnly = prefix.lastIndexOf('/') match {
      case -1  => "" // no slash => top-level
      case idx => prefix.substring(0, idx + 1)
    }

    val pattern = fullPath.substring(wildcardIndex) // e.g. "*.csv" or "???.txt"

    try {
      // 2) List all objects under that prefix
      val objects = listObjectsRecursive(bucket, prefixOnly).map(_.trim)
      // 3) Filter them by matching the wildcard pattern as a simple glob
      val regex = ("^" + globToRegex(pattern) + "$").r
      val matched = objects.filter { keyName =>
        // keyName is the full object key
        if (!keyName.startsWith(prefixOnly)) false
        else {
          val relativePart = keyName.substring(prefixOnly.length)
          regex.findFirstIn(relativePart).isDefined
        }
      }
      // Return as fully qualified s3:// or s3a:// URIs
      Right(matched.map(k => s"s3://$bucket/$k"))
    } catch {
      case NonFatal(e) =>
        Left(FileSystemError.GenericError(s"Error resolving S3 wildcard: $url => ${e.getMessage}"))
    }
  }

  /**
   * Clean up resources. In AWS SDK v2, the S3Client is safe to close, though it's often shared. We'll close it for
   * completeness.
   */
  override def stop(): Unit = {
    s3Client.close()
  }

  // --------------------------------------------------------------------------
  // Internal helpers
  // --------------------------------------------------------------------------

  /**
   * Attempt to interpret the "s3://bucket/key" URL. Return (bucket, key). We also handle "s3a://" by rewriting it as
   * "s3://".
   */
  private def parseS3Url(url: String): (String, String) = {
    val uri = new URI(url)
    val bucket = uri.getHost // e.g. "my-bucket"
    if (bucket == null) {
      throw new DASSdkInvalidArgumentException(s"Invalid S3 URL, cannot parse bucket: $url")
    }
    // path => everything after "/"
    val key = uri.getPath.stripPrefix("/")
    (bucket, key)
  }

  /**
   * Lists objects as if the path is a directory prefix. Returns the full object keys that match that prefix. If no
   * objects are found, returns either an empty list or a NotFound error if you prefer.
   */
  private def listAsDirectory(bucket: String, prefix: String, url: String): Either[FileSystemError, List[String]] = {
    val objectKeys = listObjectsRecursive(bucket, prefix)
    if (objectKeys.isEmpty) {
      Left(FileSystemError.NotFound(url))
    } else {
      // Return them as fully qualified S3 URIs
      val uris = objectKeys.map(k => s"s3://$bucket/$k")
      Right(uris)
    }
  }

  /**
   * Recursively lists objects from S3 using ListObjectsV2. If you don't want recursion, remove the repeated calls and
   * just do a single page or prefix listing.
   */
  private def listObjectsRecursive(bucket: String, prefix: String): List[String] = {
    var results = List.empty[String]
    var continuationToken: Option[String] = None

    do {
      val reqBuilder = ListObjectsV2Request.builder().bucket(bucket)
      if (prefix.nonEmpty) reqBuilder.prefix(prefix)
      continuationToken.foreach(reqBuilder.continuationToken)

      val req = reqBuilder.build()
      val resp: ListObjectsV2Response = s3Client.listObjectsV2(req)

      val pageKeys =
        resp.contents().toArray.toList.map(_.asInstanceOf[software.amazon.awssdk.services.s3.model.S3Object])
      results = results ++ pageKeys.map(_.key())

      continuationToken = Option(resp.nextContinuationToken()).filter(_.nonEmpty)
    } while (continuationToken.isDefined)

    results
  }
}

object S3FileSystem {
  def build(options: Map[String, String], cacheFolder: String): S3FileSystem = {
    val accessKey = options.get("aws_access_key")
    val secretKey = options.get("aws_secret_key")
    val region = options.get("aws_region")

    new S3FileSystem(accessKey, secretKey, region, cacheFolder)
  }
}
