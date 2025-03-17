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

package com.rawlabs.das.datafiles.filesystem.local

import java.io.{File, FileInputStream, InputStream}
import java.net.URI
import java.nio.file.{DirectoryStream, FileSystems, Files, Path, Paths}

import scala.jdk.CollectionConverters.IterableHasAsScala

import com.rawlabs.das.datafiles.filesystem.DASFileSystem

/**
 * Local filesystem implementation for "file://" or plain paths. Usage:
 *   - e.g. "file:///home/user/data.csv" or just "/home/user/data.csv" if scheme is null.
 */
class LocalFileSystem extends DASFileSystem {

  /**
   * Lists the file or directory at the given URL.
   *   - If the path is a file, return a single-element list.
   *   - If the path is a directory, return immediate child files (non-recursive).
   */
  override def list(url: String): List[String] = {
    val file = fileFromUrl(url)
    if (!file.exists()) {
      Nil
    } else if (file.isDirectory) {
      // Directory => list its immediate children (files only)
      file
        .listFiles()
        .filter(_.isFile)
        .map(_.toURI.toString)
        .toList
    } else {
      // Single file
      List(file.toURI.toString)
    }
  }

  /**
   * Opens the file for reading and returns an InputStream. Caller must close the stream.
   */
  override def open(url: String): InputStream = {
    val file = fileFromUrl(url)
    if (!file.exists() || file.isDirectory) {
      throw new IllegalArgumentException(s"LocalFileSystem cannot open $url; does not exist or is a directory.")
    }
    new FileInputStream(file)
  }

  /**
   * Resolves wildcards in the path, e.g. "file:///tmp/ *.csv" or "/tmp/ *.csv" if scheme is null. We'll do a simple
   * "glob" approach. If no wildcard, returns list() or the single file.
   */
  override def resolveWildcard(url: String): List[String] = {
    val uri = new URI(url)
    val path = Paths.get(uri) // This can handle file:// or no scheme
    val pathString = path.toString

    // If the path doesn't contain '*', '?', or other globbing symbols,
    // treat as normal => single or directory listing:
    if (!containsGlob(pathString)) {
      // Return the normal listing
      return list(url)
    }

    // If there is a glob, let's separate directory from the pattern
    // e.g. "/tmp/*.csv"
    val (dirPath, pattern) = splitDirAndPattern(path)

    // Use Java NIO DirectoryStream with a "glob" filter
    if (!Files.isDirectory(dirPath)) {
      // If the "dirPath" is not a directory, we can't do a glob listing:
      return Nil
    }

    val matcher = FileSystems.getDefault.getPathMatcher("glob:" + pattern)

    val stream: DirectoryStream[Path] = Files.newDirectoryStream(dirPath)
    try {
      stream.asScala
        .filter(p => matcher.matches(p.getFileName))
        .map(_.toUri.toString)
        .toList
    } finally {
      stream.close()
    }
  }

  /**
   * Closes the filesystem, no-op for local.
   */
  override def stop(): Unit = {
    // Nothing to do
  }

  // -----------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------

  /**
   * Helper to parse the URL into a local File object. E.g. "file:///home/user/data.csv" => new
   * File("/home/user/data.csv"). If scheme is null, same approach => new File(url).
   */
  private def fileFromUrl(url: String): File = {
    val uri = new URI(url)
    if (uri.getScheme == null || uri.getScheme == "file") {
      new File(uri)
    } else {
      // Should never happen if we consistently route "github://", "s3://" to their own classes
      throw new IllegalArgumentException(s"LocalFileSystem only supports file:// URLs or no scheme, got: $url")
    }
  }

  /**
   * Quick check if a path has wildcard symbols like '*' or '?' or '[' etc.
   */
  private def containsGlob(pathString: String): Boolean = {
    // Basic approach
    pathString.contains("*") || pathString.contains("?") || pathString.contains("[")
  }

  /**
   * Split a path like /tmp/ *.csv into ("/tmp", "*.csv"). If there's no slash or entire thing is a pattern, adjust
   * accordingly.
   */
  private def splitDirAndPattern(fullPath: Path): (Path, String) = {
    val parent = fullPath.getParent
    val fileName = fullPath.getFileName.toString
    if (parent == null) {
      // No parent => treat current directory as "."
      (Paths.get("."), fileName)
    } else {
      (parent, fileName)
    }
  }
}
