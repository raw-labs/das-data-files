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

import java.io.{FileNotFoundException, InputStream}

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import com.rawlabs.das.datafiles.filesystem.FileSystemError.{GenericError, NotFound}
import com.rawlabs.das.datafiles.filesystem.{DASFileSystem, FileSystemError}
import com.rawlabs.das.sdk.DASSdkInvalidArgumentException

class S3FileSystem(accessKey: Option[String], secretKey: Option[String], region: Option[String], cacheFolder: String)
    extends DASFileSystem(cacheFolder) {

  private val conf = new Configuration()

  // Let Hadoop know to use s3a
  conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

  if (accessKey.isDefined) {
    conf.set("fs.s3a.access.key", accessKey.get)
    conf.set(
      "fs.s3a.secret.key",
      secretKey.getOrElse(throw new DASSdkInvalidArgumentException("Missing aws secret key")))
  } else {
    conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
  }

  region.foreach(r => conf.set("fs.s3a.endpoint", s"s3.$r.amazonaws.com"))

  override def list(url: String): Either[FileSystemError, List[String]] = {
    if (!url.startsWith("s3://") || !url.startsWith("s3a://")) {
      return Left(FileSystemError.Unsupported(s"Unsupported S3 URL: $url"))
    }
    val renamed = url.replace("s3://", "s3a://")

    val hadoopPath = new Path(renamed)
    val s3Fs = FileSystem.get(hadoopPath.toUri, conf)
    try {
      val status = s3Fs.getFileStatus(hadoopPath)
      if (status.isDirectory) {
        val files = s3Fs
          .listStatus(hadoopPath)
          .filterNot(_.isDirectory)
          .map(_.getPath.toString)
          .toList

        Right(files)
      } else {
        Right(List(hadoopPath.toString))
      }
    } catch {
      case e: FileNotFoundException => Left(NotFound(url))
      case e: org.apache.hadoop.security.AccessControlException =>
        Left(FileSystemError.PermissionDenied(s"Access denied: $url => ${e.getMessage}"))
      case NonFatal(e) => Left(GenericError(s"Error listing S3 path: $url => ${e.getMessage}"))
    } finally {
      s3Fs.close()
    }
  }

  override def open(url: String): Either[FileSystemError, InputStream] = {
    if (!url.startsWith("s3://") || url.startsWith("s3a://")) {
      return Left(FileSystemError.Unsupported(s"Unsupported S3 URL: $url"))
    }
    val renamed = url.replace("s3://", "s3a://")
    val hadoopPath = new Path(renamed)
    val s3Fs = FileSystem.get(hadoopPath.toUri, conf)
    try {
      Right(s3Fs.open(hadoopPath))
    } catch {
      case e: FileNotFoundException =>
        Left(NotFound(url))
      case e: org.apache.hadoop.security.AccessControlException =>
        Left(FileSystemError.PermissionDenied(s"Access denied: $url => ${e.getMessage}"))
      case NonFatal(e) => Left(GenericError(s"Error opening S3 file: $url => ${e.getMessage}"))
    } finally {
      s3Fs.close()
    }
  }

  override def resolveWildcard(url: String): Either[FileSystemError, List[String]] = {
    val renamed = url.replace("s3://", "s3a://")
    val hadoopPath = new Path(renamed)
    val s3Fs = FileSystem.get(hadoopPath.toUri, conf)
    try {
      val matches: Array[FileStatus] = s3Fs.globStatus(hadoopPath)
      if (matches == null || matches.isEmpty) {
        Right(Nil)
      } else {
        val files = matches
          .filterNot(_.isDirectory)
          .map(_.getPath.toString)
          .toList
        Right(files)
      }
    } catch {
      case e: FileNotFoundException => Left(NotFound(url))
      case e: org.apache.hadoop.security.AccessControlException =>
        Left(FileSystemError.PermissionDenied(s"Access denied: $url => ${e.getMessage}"))
      case NonFatal(e) => Left(GenericError(s"Error resolving S3 wildcard: $url => ${e.getMessage}"))
    } finally {
      s3Fs.close()
    }
  }

  override def stop(): Unit = {}

}

object S3FileSystem {
  def build(options: Map[String, String], cacheFolder: String): S3FileSystem = {
    val accessKey = options.get("aws_access_key")
    val secretKey = options.get("aws_secret_key")
    val region = options.get("aws_region")

    new S3FileSystem(accessKey, secretKey, region, cacheFolder)
  }
}
