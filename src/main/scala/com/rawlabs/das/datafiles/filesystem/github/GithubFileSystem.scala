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

package com.rawlabs.das.datafiles.filesystem.github

import java.io.InputStream
import java.net.URI

import scala.jdk.CollectionConverters._

import org.kohsuke.github._

import com.rawlabs.das.datafiles.filesystem.{FileSystemError}
import com.rawlabs.das.datafiles.filesystem.api.BaseFileSystem

class GithubFileSystem(githubClient: GitHub, cacheFolder: String, maxDownloadSize: Long = 100L * 1024L * 1024L)
    extends BaseFileSystem(cacheFolder, maxDownloadSize) {

  private case class GithubFile(owner: String, repo: String, branch: String, path: String)

  val name: String = "github"

  override def supportsUrl(url: String): Boolean = parseGitHubUrl(url).isRight

  /**
   * Lists files at the given GitHub URL.
   *
   * It first parses the URL (expected in the format: github://owner/repo/branch/path/to/file_or_dir) and then attempts
   * to list the directory contents using Hub4j. If the given path is a file, it returns a single-element list.
   */
  override def list(url: String): Either[FileSystemError, List[String]] = {
    val file = parseGitHubUrl(url) match {
      case Left(err)   => return Left(err)
      case Right(file) => file
    }

    try {
      val repo: GHRepository = githubClient.getRepository(s"${file.owner}/${file.repo}")
      // Try to list the directory content first.
      val contents =
        try {
          repo.getDirectoryContent(file.path.stripSuffix("/"), file.branch).asScala.toList
        } catch {
          // If deserialization fails, assume it is a file and fetch file content instead.
          case e: Exception
              if Option(e.getCause).exists(
                _.isInstanceOf[com.fasterxml.jackson.databind.exc.MismatchedInputException]) =>
            List(repo.getFileContent(file.path, file.branch))
        }
      // Only include files in the result.
      val files = contents.filter(_.isFile).map { content =>
        s"github://${file.owner}/${file.repo}/${file.branch}/${content.getPath}"
      }
      Right(files)
    } catch {
      case _: GHFileNotFoundException => Left(FileSystemError.NotFound(s"File not found: $url"))
      case e: HttpException if e.getResponseCode == 404 =>
        Left(FileSystemError.NotFound(url))
      case e: HttpException if e.getResponseCode == 401 =>
        Left(FileSystemError.Unauthorized(s"Unauthorized $url => ${e.getMessage}"))
      case e: HttpException if e.getResponseCode == 403 =>
        Left(FileSystemError.PermissionDenied(s"Permission denied $url => ${e.getMessage}"))
      case e: HttpException if e.getResponseCode == 429 =>
        Left(FileSystemError.TooManyRequests(s"Too many requests $url => ${e.getMessage}"))
    }

  }

  /**
   * Opens the file at the given GitHub URL and returns an InputStream.
   *
   * The file is accessed via Hub4j, and its download URL is used to open a stream.
   */
  override def open(url: String): Either[FileSystemError, InputStream] = {
    val file = parseGitHubUrl(url) match {
      case Left(err)   => return Left(err)
      case Right(file) => file
    }

    try {
      val repo: GHRepository = githubClient.getRepository(s"${file.owner}/${file.repo}")
      val fileContent = repo.getFileContent(file.path, file.branch)
      // Use the file’s download URL to open a stream.
      val downloadUrl = fileContent.getDownloadUrl
      val inputStream = new URI(downloadUrl.toString).toURL.openStream()
      Right(inputStream)
    } catch {
      case _: GHFileNotFoundException => Left(FileSystemError.NotFound(s"File not found: $url"))
      case e: HttpException if e.getResponseCode == 404 =>
        Left(FileSystemError.NotFound(url))
      case e: HttpException if e.getResponseCode == 401 =>
        Left(FileSystemError.Unauthorized(s"Unauthorized $url => ${e.getMessage}"))
      case e: HttpException if e.getResponseCode == 403 =>
        Left(FileSystemError.PermissionDenied(s"Permission denied $url => ${e.getMessage}"))
      case e: HttpException if e.getResponseCode == 429 =>
        Left(FileSystemError.TooManyRequests(s"Too many requests $url => ${e.getMessage}"))

    }

  }

  /**
   * Resolves wildcard patterns in the GitHub URL.
   *
   * This implementation splits the URL into a prefix (without the wildcard part) and a pattern. It then lists the files
   * at the prefix and filters them based on the glob pattern.
   */
  override def resolveWildcard(url: String): Either[FileSystemError, List[String]] = {
    val (prefixUrl, maybePattern) = splitWildcard(url)
    maybePattern match {
      case None =>
        list(prefixUrl)
      case Some(pattern) =>
        list(prefixUrl).map { allFiles =>
          val regex = ("^" + prefixUrl + globToRegex(pattern) + "$").r
          // Create a regex from the glob pattern.
          allFiles.filter(regex.matches)
        }
    }
  }

  /**
   * Return the size of the GitHub file (in bytes), or an error if not found/dir.
   */
  override def getFileSize(url: String): Either[FileSystemError, Long] = {
    val file = parseGitHubUrl(url) match {
      case Left(err)   => return Left(err)
      case Right(file) => file
    }

    try {
      val repo: GHRepository = githubClient.getRepository(s"${file.owner}/${file.repo}")
      val fileContent = repo.getFileContent(file.path, file.branch)
      if (fileContent.isFile) {
        Right(fileContent.getSize) // size in bytes
      } else {
        Left(FileSystemError.Unsupported(s"Path refers to a directory, cannot get size: $url"))
      }
    } catch {
      case _: GHFileNotFoundException => Left(FileSystemError.NotFound(s"File not found: $url"))
      case e: HttpException if e.getResponseCode == 404 =>
        Left(FileSystemError.NotFound(url))
      case e: HttpException if e.getResponseCode == 401 =>
        Left(FileSystemError.Unauthorized(s"Unauthorized $url => ${e.getMessage}"))
      case e: HttpException if e.getResponseCode == 403 =>
        Left(FileSystemError.PermissionDenied(s"Permission denied $url => ${e.getMessage}"))
      case e: HttpException if e.getResponseCode == 429 =>
        Left(FileSystemError.TooManyRequests(s"Too many requests $url => ${e.getMessage}"))
    }

  }

  /**
   * Stops the filesystem. Hub4j’s GitHub client does not require an explicit shutdown, but if needed you could close
   * resources here.
   */
  override def stop(): Unit = {}

  // ----------------------------------------------------------------
  // Internal helper functions
  // ----------------------------------------------------------------

  /**
   * Parses a GitHub URL of the form: github://owner/repo/branch/path/to/file_or_dir into its components.
   */
  private def parseGitHubUrl(url: String): Either[FileSystemError, GithubFile] = {
    if (!url.startsWith("github://")) {
      Left(FileSystemError.InvalidUrl(url, s"URL must start with 'github://'"))
    } else {
      val withoutScheme = url.stripPrefix("github://")
      val parts = withoutScheme.split("/", 4).toList
      if (parts.size < 4) {
        Left(FileSystemError.InvalidUrl(url, "GitHub URL must be: github://owner/repo/branch/path/to/file"))
      } else {
        Right(GithubFile(parts.head, parts(1), parts(2), parts(3)))
      }
    }
  }

}

object GithubFileSystem {
  def build(options: Map[String, String], cacheFolder: String, maxDownloadSize: Long): GithubFileSystem = {
    val builder = new GitHubBuilder()
    options.get("github_api_token").foreach(token => builder.withOAuthToken(token))

    new GithubFileSystem(builder.build(), cacheFolder, maxDownloadSize)
  }
}
