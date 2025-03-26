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

import java.io.{File, InputStream}
import java.nio.file.{Files, StandardCopyOption}
import java.util.UUID

import com.typesafe.scalalogging.StrictLogging

/**
 * Base class for "DAS" filesystem abstractions.
 */
abstract class BaseFileSystem(downloadFolder: String, maxLocalFileSize: Long) extends StrictLogging {

  private val downloadPath = new File(downloadFolder)
  downloadPath.mkdirs()

  // The name of the filesystem, e.g. "S3", "HTTP", "Local"
  def name: String

  def supportsUrl(url: String): Boolean

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
   * Returns the size of the file in bytes. If the filesystem concept doesn't apply to "directories" or if the file is
   * not found, return an error.
   */
  def getFileSize(url: String): Either[FileSystemError, Long]

  /**
   * Gets a local path (on disk) for the given `url`. In some file systems this might require downloading; in others, it
   * can be a no-op. Default implementation downloads the file to the local cache.
   */
  def getLocalUrl(url: String): Either[FileSystemError, String] = {

    // check the file size first
    getFileSize(url) match {
      case Left(err) => return Left(err)
      case Right(actualSize) if actualSize > maxLocalFileSize =>
        logger.warn(s"File $url is too large ($actualSize bytes), downloading aborted")
        return Left(FileSystemError.FileTooLarge(url, actualSize, maxLocalFileSize))
      case _ => // File size is OK
    }

    val uniqueName = UUID.randomUUID().toString
    val outFile = new File(downloadFolder, uniqueName)
    logger.debug(s"Downloading $url to $outFile")
    val inputStream = open(url) match {
      case Right(is) => is
      case Left(err) => return Left(err)
    }
    try {
      Files.copy(inputStream, outFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      Right(outFile.getAbsolutePath)
    } finally {
      inputStream.close()
    }
  }

  /**
   * Cleanly shuts down / closes resources if needed.
   */
  def stop(): Unit

  /**
   * Splits the URL into a prefix and an optional wildcard pattern. For example, given
   * owner/repo/branch/path/data*.csv", it returns ("owner/repo/branch/path", Some("data*.csv")). It there is no
   * wildcard, returns the original URL and None.
   */
  protected def splitWildcard(url: String): (String, Option[String]) = {
    val lastSlash = url.lastIndexOf('/')
    val (folder, candidate) = if (lastSlash < 0) {
      "" -> url
    } else {
      url.substring(0, lastSlash + 1) -> url.substring(lastSlash + 1)
    }

    if (candidate.contains("*") || candidate.contains("?"))
      (folder, Some(candidate))
    else
      (url, None)

  }

  /**
   * Converts a simple glob (with *, ?) into a corresponding regex string. E.g. "*.csv" => ".*\.csv"
   *
   * This is simplistic and doesn't handle bracket expressions ([abc]) or other advanced globs.
   */
  protected def globToRegex(glob: String): String = {
    // Escape all regex metacharacters except the glob-related (*, ?).
    // We escape: \ ^ $ . + | { } ( ) [ ]

    glob
      // Escape all regex special characters except '*' and '?'
      .replaceAll("([\\^\\$\\.\\+\\|\\(\\)\\{\\}\\[\\]\\\\])", """\\$1""")
      // Convert '?' (glob) to '[^/]' (regex) avoid matching directory separators
      .replace("?", "[^/]")
      // Convert '*' (glob) to '[^/]*' (regex) avoid matching directory separators
      .replace("*", "[^/]*")

  }

}
