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

package com.rawlabs.das.datafiles.utils

import com.rawlabs.das.datafiles.filesystem.{AwsSecretCredential, FileSystemCredential, GithubApiTokenCredential}
import com.rawlabs.das.sdk.DASSdkInvalidArgumentException

/**
 * Represents a single path’s configuration
 */

case class PathConfig(url: String, maybeName: Option[String], format: Option[String], options: Map[String, String])

/**
 * Holds all parsed config from user’s definition for the entire DAS.
 */
class DASDataFilesOptions(options: Map[String, String]) {

  // Number of paths to load, e.g. paths=3 => path0_..., path1_..., path2_...
  val nrPaths: Int = options.get("paths").map(_.toInt).getOrElse(1)

  val fileSystemCredential: Option[FileSystemCredential] = {
    if (options.contains("aws_access_key")) {
      val accessKey = options("aws_access_key")
      val secretKey =
        options.getOrElse("aws_secret_key", throw new DASSdkInvalidArgumentException("aws_secret_key not found"))

      val region = options.getOrElse("aws_region", throw new DASSdkInvalidArgumentException("aws_region not found"))
      Some(AwsSecretCredential(region, accessKey, secretKey))

    } else if (options.contains("github_api_token")) {
      val apiToken = options("github_api_token")
      Some(GithubApiTokenCredential(apiToken))
    } else {
      None
    }

  }

  /**
   * Build a list of DataFileConfig from user’s config keys.
   *   - path0_url, path0_format, path0_name (optional), ...
   *   - path1_url, path1_format, path1_name (optional), ...
   */
  val pathConfig: Seq[PathConfig] = {
    (0 until nrPaths).map { i =>
      val prefix = s"path${i}_"

      // Mandatory fields
      val url =
        options.getOrElse(
          prefix + "url",
          throw new DASSdkInvalidArgumentException(s"Missing '${prefix}url' option for DataFile DAS."))
      val format = options.get(prefix + "format")

      // Name is optional: if not provided, derive from URL
      val maybeName = options.get(prefix + "name")

      // Gather all prefixed options into a sub-map for this path
      val pathOptions = options.collect {
        case (k, v) if k.startsWith(prefix) => (k.drop(prefix.length), v)
      }

      PathConfig(maybeName = maybeName, url = url, format = format, options = pathOptions)
    }
  }

}
