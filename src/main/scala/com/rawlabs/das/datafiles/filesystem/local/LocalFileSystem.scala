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

package com.rawlabs.das.datafiles.filesystem.local

import java.io.{File, FileInputStream, FileNotFoundException, InputStream}
import java.net.{URI, URISyntaxException}
import java.nio.file._
import java.util.regex.PatternSyntaxException

import scala.jdk.CollectionConverters._

import com.rawlabs.das.datafiles.filesystem.FileSystemError
import com.rawlabs.das.datafiles.filesystem.api.BaseFileSystem

class LocalFileSystem extends BaseFileSystem() {

  val name: String = "local"

  override def supportsUrl(url: String): Boolean = fileFromUrl(url).isRight

  override def list(url: String): Either[FileSystemError, List[String]] = {
    val file = fileFromUrl(url) match {
      case Left(err) => return Left(err)
      case Right(f)  => f
    }

    if (!file.exists()) {
      Left(FileSystemError.NotFound(url, s"File not found $url"))
    } else if (file.isDirectory) {
      // Directory => list immediate children (files only)
      val files = file
        .listFiles()
        .filter(_.isFile)
        .map(_.toURI.toString)
        .toList

      Right(files)
    } else {
      // Single file
      Right(List(file.toURI.toString))
    }
  }

  override def open(url: String): Either[FileSystemError, InputStream] = {
    try {
      val file = fileFromUrl(url) match {
        case Left(err) => return Left(err)
        case Right(f)  => f
      }

      if (!file.exists()) {
        Left(FileSystemError.NotFound(url, s"File not found $url"))
      } else if (file.isDirectory) {
        Left(FileSystemError.Unsupported(s"Cannot open directory ($url) as file"))
      } else {
        Right(new FileInputStream(file))
      }
    } catch {
      case e: FileNotFoundException if e.getMessage.contains("Permission denied") =>
        Left(FileSystemError.PermissionDenied("Permission denied for ($url)"))
    }
  }

  override def resolveWildcard(url: String): Either[FileSystemError, List[String]] = {
    try {
      val file = fileFromUrl(url) match {
        case Left(err) => return Left(err)
        case Right(f)  => f
      }

      val path = file.toPath
      val pathString = path.toString

      // If the path doesn't contain any glob symbol, treat it as normal:
      if (!containsGlob(pathString)) {
        // Return the normal listing
        list(url)
      } else {
        // We have a glob => separate directory from the pattern
        val (dirPath, pattern) = splitDirAndPattern(path)

        if (!Files.isDirectory(dirPath)) {
          // If "dirPath" is not a directory, we can't do a glob listing:
          Right(Nil)
        } else {
          val matcher = FileSystems.getDefault.getPathMatcher("glob:" + pattern)
          val stream: DirectoryStream[Path] = Files.newDirectoryStream(dirPath)
          try {
            val matched = stream.asScala
              .filter(p => matcher.matches(p.getFileName))
              .map(_.toUri.toString)
              .toList

            Right(matched)
          } finally {
            stream.close()
          }
        }
      }
    } catch {
      case e: PatternSyntaxException =>
        Left(FileSystemError.Unsupported("Error in glob pattern: " + e.getMessage))
      case e: UnsupportedOperationException =>
        Left(FileSystemError.Unsupported(e.getMessage))
      case e: FileNotFoundException =>
        Left(FileSystemError.NotFound(url, s"File not found $url: ${e.getMessage}"))
    }
  }

  override def stop(): Unit = {}

  /**
   * Return the size of the file in bytes, or an error if not found or a directory.
   */
  override def getFileSize(url: String): Either[FileSystemError, Long] = {
    val file = fileFromUrl(url) match {
      case Left(err) => return Left(err)
      case Right(f)  => f
    }

    if (!file.exists()) {
      Left(FileSystemError.NotFound(url, s"File not found $url"))
    } else if (file.isDirectory) {
      Left(FileSystemError.Unsupported(s"Cannot get size of a directory ($url)"))
    } else {
      Right(file.length()) // length in bytes
    }
  }

  // -----------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------

  /**
   * Attempt to parse the URL into a local File object. If the scheme is invalid, return Left().
   */
  private def fileFromUrl(url: String): Either[FileSystemError, File] = {
    try {
      val uri = new URI(url)
      if (uri.getScheme == null) {
        Right(new File(url))
      } else if (uri.getScheme == "file") {
        Right(new File(uri))
      } else {
        Left(FileSystemError.Unsupported(s"LocalFileSystem only supports file:// URLs or no scheme but got: $url"))
      }
    } catch {
      case e: URISyntaxException =>
        Left(FileSystemError.InvalidUrl(url, e.getMessage))
    }
  }

  private def containsGlob(pathString: String): Boolean = {
    pathString.contains("*") || pathString.contains("?") || pathString.contains("[")
  }

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
