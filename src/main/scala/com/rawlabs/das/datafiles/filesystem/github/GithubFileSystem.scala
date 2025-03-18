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

package com.rawlabs.das.datafiles.filesystem.github

import java.io.InputStream
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.rawlabs.das.datafiles.filesystem.FileSystemError
import com.rawlabs.das.datafiles.filesystem._

class GithubFileSystem(
    authToken: Option[String],
    cacheFolder: String,
    connectTimeoutMs: Int = 5000,
    readTimeoutMs: Int = 30000)
    extends DASFileSystem(cacheFolder) {

  private val httpClient =
    HttpClient
      .newBuilder()
      .connectTimeout(Duration.ofMillis(connectTimeoutMs))
      .build()

  private val objectMapper = new ObjectMapper()

  override def list(url: String): Either[FileSystemError, List[String]] = {
    val (owner, repo, branch, subPath) = parseGitHubUrl(url) match {
      case Left(err)     => return Left(err)
      case Right(parsed) => parsed
    }

    val apiUrl = s"https://api.github.com/repos/$owner/$repo/contents/$subPath?ref=$branch"

    fetchJson(apiUrl).flatMap { rootNode =>
      if (rootNode.isArray) {
        // Directory listing => ArrayNode
        val arrayNode = rootNode.asInstanceOf[ArrayNode]
        val files = arrayNode
          .iterator()
          .asScala
          .collect {
            case node if node.has("type") && node.get("type").asText() == "file" =>
              val filePath = node.get("path").asText() // e.g. "docs/README.md"
              s"github://$owner/$repo/$branch/$filePath"
          }
          .toList

        Right(files)
      } else if (rootNode.isObject) {
        val nodeType = Option(rootNode.get("type")).map(_.asText()).getOrElse("")
        if (nodeType == "file") {
          Right(List(s"github://$owner/$repo/$branch/$subPath"))
        } else {
          // It's not a file => could be a directory with no children or an error
          Right(Nil)
        }
      } else {
        // Unrecognized response
        Right(Nil)
      }
    }
  }

  override def open(url: String): Either[FileSystemError, InputStream] = {
    val (owner, repo, branch, filePath) = parseGitHubUrl(url) match {
      case Left(err)     => return Left(err)
      case Right(parsed) => parsed
    }
    val rawUrl = s"https://raw.githubusercontent.com/$owner/$repo/$branch/$filePath"

    val requestBuilder = HttpRequest
      .newBuilder(URI.create(rawUrl))
      .timeout(Duration.ofMillis(readTimeoutMs))
      .GET()

    authToken.foreach { token =>
      requestBuilder.header("Authorization", s"Bearer $token")
    }

    val request = requestBuilder.build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream())
    val code = response.statusCode()

    if (code >= 200 && code < 300) {
      Right(response.body())
    } else {
      // Map to error type
      response.body().close() // discard the stream if error
      code match {
        case 401 => Left(FileSystemError.Unauthorized(s"Unauthorized to open $rawUrl"))
        case 403 => Left(FileSystemError.PermissionDenied(s"Forbidden to open $rawUrl"))
        case 404 => Left(FileSystemError.NotFound(url))
        case _   => Left(FileSystemError.GenericError(s"Error $code while opening $rawUrl"))
      }
    }
  }

  override def resolveWildcard(url: String): Either[FileSystemError, List[String]] = {
    val (prefixPath, maybePattern) = splitWildcard(url)

    maybePattern match {
      case None =>
        // No wildcard => just list everything
        list(prefixPath)

      case Some(pattern) =>
        list(prefixPath).map { allFiles =>
          // Filter by pattern. A simple approach:
          val regex = ("^" + pattern.replace(".", "\\.").replace("*", ".*") + "$").r
          allFiles.filter { fileUrl =>
            val (_, _, _, fileName) = parseGitHubUrl(fileUrl).getOrElse(("", "", "", ""))
            regex.findFirstIn(fileName).isDefined
          }
        }
    }
  }

  override def stop(): Unit = {
    httpClient.close()
  }

  // ----------------------------------------------------------------
  // Internal helpers
  // ----------------------------------------------------------------

  private def fetchJson(apiUrl: String): Either[FileSystemError, JsonNode] = {
    val requestBuilder = HttpRequest
      .newBuilder(URI.create(apiUrl))
      .timeout(Duration.ofMillis(readTimeoutMs))
      .GET()

    authToken.foreach { token =>
      requestBuilder.header("Authorization", s"Bearer $token")
    }

    requestBuilder.header("Accept", "application/vnd.github.v3+json")

    val request = requestBuilder.build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    val code = response.statusCode()
    if (code >= 200 && code < 300) {
      Right(objectMapper.readTree(response.body()))
    } else {
      code match {
        case 401 => Left(FileSystemError.Unauthorized(s"Unauthorized for $apiUrl"))
        case 403 => Left(FileSystemError.PermissionDenied(s"Forbidden for $apiUrl"))
        case 404 => Left(FileSystemError.NotFound(apiUrl))
        case _   => Left(FileSystemError.GenericError(s"GitHub error $code for $apiUrl"))
      }
    }
  }

  private def parseGitHubUrl(url: String): Either[FileSystemError, (String, String, String, String)] = {
    if (!url.startsWith("github://")) {
      Left(FileSystemError.Unsupported(s"URL must start with github://, got: $url"))
    } else {
      val withoutScheme = url.stripPrefix("github://")
      val parts = withoutScheme.split("/", 4).toList
      if (parts.size < 4) {
        Left(FileSystemError.Unsupported("GitHub URL must be: github://owner/repo/branch/path/to/file"))
      } else {
        Right((parts.head, parts(1), parts(2), parts(3)))
      }
    }
  }

  private def splitWildcard(fullUrl: String): (String, Option[String]) = {
    val lastSlash = fullUrl.lastIndexOf('/')
    if (lastSlash < 0) (fullUrl, None)
    else {
      val candidate = fullUrl.substring(lastSlash + 1)
      if (candidate.contains("*")) {
        val prefix = fullUrl.substring(0, lastSlash)
        (prefix, Some(candidate))
      } else (fullUrl, None)
    }
  }
}

object GithubFileSystem {

  def build(options: Map[String, String], cacheFolder: String): GithubFileSystem = {
    val authToken = options.get("github_api_token")
    new GithubFileSystem(authToken, cacheFolder)
  }
}
