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

import java.io.InputStream
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

import scala.jdk.CollectionConverters._
import scala.util.Try

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

/**
 * Example GitHub-based FileSystemApi using Jackson for JSON parsing.
 *
 * Expects a path format like: github://owner/repo/branch/path/to/file
 *
 * @param authToken optional GitHub Personal Access Token (private repos or higher rate limits)
 * @param connectTimeoutMs connection timeout in milliseconds
 * @param readTimeoutMs read (per-request) timeout in milliseconds
 */
class GitHubFileSystem(authToken: Option[String] , connectTimeoutMs: Int = 5000, readTimeoutMs: Int = 30000)
    extends FileSystemApi {

  private val httpClient: HttpClient =
    HttpClient
      .newBuilder()
      .connectTimeout(Duration.ofMillis(connectTimeoutMs))
      .build()

  private val objectMapper = new ObjectMapper()

  /**
   * Lists files for the given "github://..." path. If `path` references a directory, returns all files in that
   * directory (non-recursive). If it's a single file, returns a singleton list with that file.
   */
  override def list(url: String): List[String] = {
    val (owner, repo, branch, subPath) = parseGitHubUrl(url)
    // GitHub Contents API endpoint:
    // GET /repos/{owner}/{repo}/contents/{subPath}?ref={branch}
    val apiUrl = s"https://api.github.com/repos/$owner/$repo/contents/$subPath?ref=$branch"

    val rootNode = fetchJson(apiUrl)

    if (rootNode.isArray) {
      // Directory listing => ArrayNode
      val arrayNode = rootNode.asInstanceOf[ArrayNode]
      arrayNode
        .iterator()
        .asScala
        .collect {
          case node if node.has("type") && node.get("type").asText() == "file" =>
            val filePath = node.get("path").asText() // e.g. "docs/README.md"
            s"github://$owner/$repo/$branch/$filePath"
        }
        .toList
    } else if (rootNode.isObject) {
      // Possibly a single file
      val nodeType = Option(rootNode.get("type")).map(_.asText()).getOrElse("")
      if (nodeType == "file") {
        List(s"github://$owner/$repo/$branch/$subPath")
      } else {
        // It's not a file => could be a directory with no children or an error
        Nil
      }
    } else {
      // Unrecognized response
      Nil
    }
  }

  /**
   * Opens the raw file from GitHub as an InputStream. Caller is responsible for closing the stream.
   */
  override def open(url: String): InputStream = {
    val (owner, repo, branch, filePath) = parseGitHubUrl(url)
    // e.g. https://raw.githubusercontent.com/owner/repo/branch/path/to/file
    val rawUrl = s"https://raw.githubusercontent.com/$owner/$repo/$branch/$filePath"

    val requestBuilder = HttpRequest
      .newBuilder(URI.create(rawUrl))
      .timeout(Duration.ofMillis(readTimeoutMs))
      .GET()

    authToken.foreach { token =>
      requestBuilder.header("Authorization", s"Bearer $token")
    }

    val response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofInputStream())
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      Try(response.body().close()) // clean up if error
      throw new RuntimeException(s"Failed to open GitHub file ($rawUrl) - HTTP status: ${response.statusCode()}")
    }

    // Return the input stream to the caller
    response.body()
  }

  /**
   * Resolves wildcards by listing files in the directory, then filtering by pattern (simple `*` matching on the final
   * path segment).
   */
  override def resolveWildcard(url: String): List[String] = {
    val (prefixPath, maybePattern) = splitWildcard(url)
    maybePattern match {
      case None =>
        // No wildcard => just list everything
        list(prefixPath)
      case Some(pattern) =>
        val wildcardRegex = ("^" + pattern.replace(".", "\\.").replace("*", ".*") + "$").r
        list(prefixPath).filter { fileUrl =>
          val (_, _, _, fileName) = parseGitHubUrl(fileUrl)
          wildcardRegex.findFirstIn(fileName).isDefined
        }
    }
  }

  /**
   * No-op for GitHub. (Java 11 HttpClient does not require explicit shutdown.)
   */
  override def stop(): Unit = {
    // nothing to do
  }

  // ----------------------------------------------------------------
  // Internal helper methods
  // ----------------------------------------------------------------

  /**
   * Fetches JSON from the GitHub API and parses it with Jackson.
   */
  private def fetchJson(apiUrl: String): JsonNode = {
    val builder = HttpRequest
      .newBuilder(URI.create(apiUrl))
      .timeout(Duration.ofMillis(readTimeoutMs))
      .GET()

    authToken.foreach { token =>
      builder.header("Authorization", s"Bearer $token")
    }

    builder.header("Accept", "application/vnd.github.v3+json")

    val response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() >= 200 && response.statusCode() < 300) {
      objectMapper.readTree(response.body())
    } else {
      throw new RuntimeException(s"GitHub API request failed: HTTP ${response.statusCode()} => $apiUrl")
    }
  }

  /**
   * Parses a path of the form: github://owner/repo/branch/path/to/file
   *
   * Returns (owner, repo, branch, path).
   */
  private def parseGitHubUrl(url: String): (String, String, String, String) = {
    require(url.startsWith("github://"), s"URL must start with github://, got: $url")
    val withoutScheme = url.stripPrefix("github://")
    val parts = withoutScheme.split("/", 4).toList
    if (parts.size < 4) {
      throw new IllegalArgumentException("GitHub URL must be: github://owner/repo/branch/path/to/file")
    }
    (parts.head, parts(1), parts(2), parts(3))
  }

  /**
   * Splits a path into a prefix and an optional wildcard pattern.
   *
   * For example, "github://owner/repo/branch/dir/ *" => ("github://owner/repo/branch/dir", Some("*"))
   */
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
