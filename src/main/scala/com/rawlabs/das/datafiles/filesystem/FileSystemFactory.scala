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

package com.rawlabs.das.datafiles.filesystem

import java.net.URI

import com.rawlabs.das.datafiles.filesystem.github.GithubFileSystem
import com.rawlabs.das.datafiles.filesystem.local.LocalFileSystem
import com.rawlabs.das.datafiles.filesystem.s3.S3FileSystem
import com.typesafe.config.ConfigFactory

object FileSystemFactory {

  private val config = ConfigFactory.load()
  private val cacheFolder = config.getString("raw.das.data-files.cache-dir")

  /**
   * Build the appropriate DASFileSystem based on the URI scheme.
   *
   * @param uri The parsed URI (e.g. s3://, github://, file://, or null scheme).
   * @param options Additional configuration for the FS (credentials, tokens, etc.).
   * @return A DASFileSystem instance (S3, GitHub, Local, etc.).
   */
  def build(uri: URI, options: Map[String, String]): DASFileSystem = {
    uri.getScheme match {
      case "s3" =>
        // Use your existing S3 builder. (S3FileSystem.build takes a Map[String, String])
        S3FileSystem.build(options, cacheFolder)

      case "github" =>
        // Use your existing GitHub builder. (GithubFileSystem.build also takes a Map)
        GithubFileSystem.build(options, cacheFolder)

      case "file" | null =>
        // "file" or a missing scheme => local filesystem
        // (null occurs if user’s URL is just "/path/to/data.csv")
        new LocalFileSystem(cacheFolder)

      case other =>
        throw new IllegalArgumentException(s"Unsupported URI scheme '$other' for path: ${uri.toString}")
    }
  }
}
