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

import java.io.{File, IOException, InputStream}
import java.nio.file.{Files, StandardCopyOption}
import java.util.UUID

import com.rawlabs.das.datafiles.filesystem.FileSystemError.GenericError

/**
 * Base class for "DAS" filesystem abstractions.
 */
abstract class DASFileSystem(downloadFolder: String) {

  /**
   * Lists files at `url`. On success, returns a list of full paths or URIs.
   */
  def list(url: String): Either[FileSystemError, List[String]]

  /**
   * Opens the file at `url` and returns an InputStream. Caller must close.
   */
  def open(url: String): Either[FileSystemError, InputStream]

  /**
   * Resolves wildcard patterns in `url` (like "*.csv"), returning the matched files.
   */
  def resolveWildcard(url: String): Either[FileSystemError, List[String]]

  /**
   * Cleanly shuts down / closes resources if needed.
   */
  def stop(): Unit

  /**
   * Gets a local path (on disk) for the given `url`. In some file systems this might require downloading; in others, it
   * can be a no-op.
   */
  def getLocalUrl(url: String): Either[FileSystemError, String] = {
    val uniqueName = UUID.randomUUID().toString
    val outFile = new File(downloadFolder, uniqueName)
    outFile.mkdirs()
    val inputStream = open(url) match {
      case Right(is) => is
      case Left(err) => return Left(err)
    }
    try {
      Files.copy(inputStream, outFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      Right(outFile.getAbsolutePath)
    } catch {
      case e: IOException =>
        Left(GenericError(s"Error getting local url: ${e.getMessage}"))
    } finally {
      inputStream.close()
    }
  }

}
