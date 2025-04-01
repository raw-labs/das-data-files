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

package com.rawlabs.das.datafiles.filesystem

import java.net.URI

import com.rawlabs.das.datafiles.filesystem.api.BaseFileSystem
import com.rawlabs.das.datafiles.filesystem.github.GithubFileSystem
import com.rawlabs.das.datafiles.filesystem.local.LocalFileSystem
import com.rawlabs.das.datafiles.filesystem.s3.S3FileSystem
import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSettings}

object FileSystemFactory {

  /**
   * Build the appropriate DASFileSystem based on the URI scheme.
   *
   * @param uri The parsed URI (e.g. s3://, github://, file://, or null scheme).
   * @param options Additional configuration for the FS (credentials, tokens, etc.).
   * @return A DASFileSystem instance (S3, GitHub, Local, etc.).
   */
  def build(uri: URI, options: Map[String, String])(implicit config: DASSettings): BaseFileSystem = {
    val cacheFolder = config.getString("das.data-files.cache-dir")
    val allowLocal = config.getBoolean("das.data-files.allow-local-files")
    val maxDownloadSize = config.getBytes("das.data-files.max-download-size")
    uri.getScheme match {
      case "s3" =>
        S3FileSystem.build(options, cacheFolder, maxDownloadSize)

      case "github" =>
        GithubFileSystem.build(options, cacheFolder, maxDownloadSize)

      case "file" | null =>
        // "file" or a missing scheme => local filesystem
        // (null occurs if user’s URL is just "/path/to/data.csv")
        if (!allowLocal) {
          throw new DASSdkInvalidArgumentException("Local files are not allowed.")
        }
        new LocalFileSystem(cacheFolder, maxDownloadSize)

      case other =>
        throw new DASSdkInvalidArgumentException(s"Unsupported URI scheme '$other' for path: ${uri.toString}")
    }
  }
}
