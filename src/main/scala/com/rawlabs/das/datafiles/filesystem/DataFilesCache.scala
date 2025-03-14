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

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class DataFilesCache(cacheDirStr: String, filesystem: FileSystemApi) {

  private val uniqueDirName = UUID.randomUUID().toString.take(8)
  private val cacheDir = Paths.get(cacheDirStr, uniqueDirName).toFile
  if (!cacheDir.exists()) cacheDir.mkdirs()

  private val acquiredFiles = new ConcurrentHashMap[URI, File]()

  def acquire(uri: URI): String = {
    if (uri.getScheme == "file") {
      // local files are not cached
      uri.getPath
    } else if (uri.getScheme == "s3") {
      // for S3, we don't need to cache either, we exchange the s3:// scheme with s3a://
      "s3a://" + uri.getAuthority + uri.getPath
    } else {
      val baseName = Paths.get(uri.getPath).getFileName.toString
      val uniqueName = s"${UUID.randomUUID().toString.take(8)}-$baseName"
      val outFile = new File(cacheDir, uniqueName)
      val inStream = filesystem.open(uri.toString)
      try {
        Files.copy(inStream, outFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      } finally {
        inStream.close()
      }
      acquiredFiles.put(uri, outFile)
      outFile.getAbsolutePath
    }
  }

  def release(uri: URI): Unit = {
    if (uri.getScheme == "s3") {
      // Do nothing
    } else {
      val file = acquiredFiles.remove(uri)
      if (file.exists()) {
        file.delete()
      }
    }
  }
}

object DataFilesCache {
  private val config: Config = ConfigFactory.load()
  private val cacheDirStr = config.getString("raw.das.data-files.cache-dir")

  def apply(filesystem: FileSystemApi): DataFilesCache = new DataFilesCache(cacheDirStr, filesystem)
}
