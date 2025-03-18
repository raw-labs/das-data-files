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

class S3FileSystem(accessKey: Option[String], secretKey: Option[String], region: Option[String], cacheFolder: String)
    extends DASFileSystem(cacheFolder) {

  private val conf = new Configuration()

  // Let Hadoop know to use s3a
  conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

  // Optionally set AWS credentials
  accessKey.foreach(ak => conf.set("fs.s3a.access.key", ak))
  secretKey.foreach(sk => conf.set("fs.s3a.secret.key", sk))
  region.foreach(r => conf.set("fs.s3a.endpoint", s"s3.$r.amazonaws.com"))

  private val s3Fs: FileSystem = FileSystem.get(conf)

  override def list(path: String): Either[FileSystemError, List[String]] = {
    val hadoopPath = new Path(path)
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
      case e: FileNotFoundException => Left(NotFound(path))
      case e: org.apache.hadoop.security.AccessControlException =>
        Left(FileSystemError.PermissionDenied(s"Access denied: $path => ${e.getMessage}"))
      case NonFatal(e) => Left(GenericError(s"Error listing S3 path: $path => ${e.getMessage}"))
    }
  }

  override def open(path: String): Either[FileSystemError, InputStream] = {
    val hadoopPath = new Path(path)
    try {
      Right(s3Fs.open(hadoopPath))
    } catch {
      case e: FileNotFoundException =>
        Left(NotFound(path))
      case e: org.apache.hadoop.security.AccessControlException =>
        Left(FileSystemError.PermissionDenied(s"Access denied: $path => ${e.getMessage}"))
      case NonFatal(e) => Left(GenericError(s"Error opening S3 file: $path => ${e.getMessage}"))
    }
  }

  override def resolveWildcard(path: String): Either[FileSystemError, List[String]] = {
    val hadoopPath = new Path(path)
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
      case e: FileNotFoundException => Left(NotFound(path))
      case e: org.apache.hadoop.security.AccessControlException =>
        Left(FileSystemError.PermissionDenied(s"Access denied: $path => ${e.getMessage}"))
      case NonFatal(e) => Left(GenericError(s"Error resolving S3 wildcard: $path => ${e.getMessage}"))
    }
  }

  override def stop(): Unit = {
    s3Fs.close()
  }

  override def getLocalUrl(path: String): Either[FileSystemError, String] = {
    // If you want to actually download S3 file to local cache, you'd do that here,
    // or you can mimic the approach from the base class, but returning Either.
    // For demonstration, let's say we do the same approach as base:
    // We'll just let the base "open" + copy. But let's keep it simple:
    // => Use the base class approach if you want.
    // For now, assume we just want to physically copy it to localFolder:
    import java.util.UUID
    import java.nio.file.{Files, StandardCopyOption}
    import java.io.File

    open(path) match {
      case Left(err) =>
        Left(err)
      case Right(stream) =>
        try {
          val uniqueName = UUID.randomUUID().toString
          val outFile = new File(cacheFolder, uniqueName)
          Files.copy(stream, outFile.toPath, StandardCopyOption.REPLACE_EXISTING)
          Right(outFile.getAbsolutePath)
        } catch {
          case e: FileNotFoundException => Left(NotFound(path))
          case e: org.apache.hadoop.security.AccessControlException =>
            Left(FileSystemError.PermissionDenied(s"Access denied: $path => ${e.getMessage}"))
          case NonFatal(e) => Left(GenericError(s"Error resolving S3 wildcard: $path => ${e.getMessage}"))
        } finally {
          stream.close()
        }
    }
  }
}

object S3FileSystem {

  /**
   * Adjusted so it returns Either.
   */
  def build(options: Map[String, String], cacheFolder: String): S3FileSystem = {
    val accessKey = options.get("aws_access_key")
    val secretKey = options.get("aws_secret_key")
    val region = options.get("aws_region")

    new S3FileSystem(accessKey, secretKey, region, cacheFolder)
  }
}
