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

import java.io.InputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import com.rawlabs.das.datafiles.filesystem.DASFileSystem

/**
 * A simple FileSystemApi for S3 using Hadoop's S3AFileSystem.
 *
 * @param accessKey AWS access key (or None to rely on the default credential chain).
 * @param secretKey AWS secret key (or None to rely on the default credential chain).
 * @param region AWS region (e.g. "us-east-1").
 *
 * Usage:
 *   - Use s3a://bucket/path in your calls to list, open, etc.
 *   - Example: "s3a://my-bucket/data/file.csv"
 */
class S3FileSystem(accessKey: Option[String], secretKey: Option[String], region: Option[String], cacheFolder: String)
    extends DASFileSystem(cacheFolder) {

  // ---------------------------------------------------------
  // Hadoop Configuration Setup
  // ---------------------------------------------------------

  private val conf = new Configuration()

  // Let Hadoop know to use the s3a client
  conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

  // Optionally set AWS credentials (if not relying on default AWS providers)
  accessKey.foreach(ak => conf.set("fs.s3a.access.key", ak))
  secretKey.foreach(sk => conf.set("fs.s3a.secret.key", sk))
  // Region-based endpoint (if you want to explicitly set it)
  // E.g. "s3.us-east-1.amazonaws.com" or "s3.eu-west-1.amazonaws.com"
  region.foreach(r => conf.set("fs.s3a.endpoint", s"s3.$r.amazonaws.com"))

  // ---------------------------------------------------------
  // Initialize the Hadoop FileSystem for S3
  // ---------------------------------------------------------

  // We can pick any valid S3 URI to initialize, or just do a generic get
  private val s3Fs: FileSystem = FileSystem.get(conf)

  // ---------------------------------------------------------
  // API Methods
  // ---------------------------------------------------------

  /**
   * Lists the files under a given S3 path. If the path is a directory, returns all file objects (non-recursive). If the
   * path is a file, returns a single-element list containing that file.
   *
   * @param path "s3a://bucket/path..."
   */
  override def list(path: String): List[String] = {
    val hadoopPath = new Path(path)
    val status = s3Fs.getFileStatus(hadoopPath)

    if (status.isDirectory) {
      // Directory => list its immediate children
      s3Fs
        .listStatus(hadoopPath)
        .filterNot(_.isDirectory)
        .map(_.getPath.toString)
        .toList
    } else {
      // It's a file => return single-element list
      List(hadoopPath.toString)
    }
  }

  /**
   * Opens a file for reading and returns an InputStream. Caller must close the stream.
   */
  override def open(path: String): InputStream = {
    val hadoopPath = new Path(path)
    s3Fs.open(hadoopPath)
  }

  /**
   * Resolves wildcards in the path by using Hadoop's globStatus. e.g. "s3a://my-bucket/data/ *.csv"
   */
  override def resolveWildcard(path: String): List[String] = {
    val hadoopPath = new Path(path)

    // globStatus returns null if no match
    val matches: Array[FileStatus] = s3Fs.globStatus(hadoopPath)
    if (matches == null || matches.isEmpty) {
      Nil
    } else {
      matches
        .filterNot(_.isDirectory)
        .map(_.getPath.toString)
        .toList
    }
  }

  /**
   * Closes the underlying FileSystem (recommended if this is a long-lived object).
   */
  override def stop(): Unit = {
    s3Fs.close()
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
