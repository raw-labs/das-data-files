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

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

import java.io.File
import java.net.URI
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import javax.net.ssl.{SSLContext, SSLParameters, TrustManager, X509TrustManager}

/**
 * Uniquely identifies an HTTP request by method, URL, optional body, and headers.
 */
case class HttpCacheKey(method: String, url: String, body: Option[String], headers: Map[String, String])

/**
 * A reference-counted HTTP file cache.
 *
 * Acquire a file via `acquireFor`, incrementing usage. Release it via `releaseFile`. Files idle longer than
 * `idleTimeoutMillis` are removed.
 *
 * @param cacheDirStr Directory path for storing downloaded files.
 * @param idleTimeoutMillis Idle time in ms before unused files are removed.
 * @param evictionCheckInterval Interval in ms to run the eviction checks.
 * @param httpOptions Basic HTTP settings (redirects, timeouts, SSL trust).
 */
class HttpFileCache(
    cacheDirStr: String,
    idleTimeoutMillis: Long,
    evictionCheckInterval: Long,
    httpOptions: HttpConnectionOptions)
    extends StrictLogging {

  // Validate input arguments.
  require(idleTimeoutMillis >= 0, "idleTimeoutMillis must be positive")
  require(evictionCheckInterval > 0, "evictionCheckInterval must be greater than zero")
  require(httpOptions.connectTimeout > 0, "httpOptions.connectTimeout must be greater than zero")

  /**
   * Subdirectory used to store cached files, ensuring uniqueness within the main cacheDirStr.
   */
  private val uniqueDirName = UUID.randomUUID().toString.take(8)
  private val cacheDir = Paths.get(cacheDirStr, uniqueDirName).toFile
  if (!cacheDir.exists()) cacheDir.mkdirs()

  /**
   * Tracks usage count and release time for each cached file.
   */
  private case class CacheEntry(localPath: String, var usageCount: Int, var lastReleaseTime: Long, var fileSize: Long)

  // Primary map from HttpCacheKey to CacheEntry
  private val cacheIndex = new ConcurrentHashMap[HttpCacheKey, CacheEntry]()

  // Single-thread scheduler for eviction tasks
  private val scheduler = Executors.newSingleThreadScheduledExecutor()
  initScheduler()

  // Build the HttpClient with optional SSL trust-all, timeouts, etc.
  private val httpClient = {
    val builder = HttpClient.newBuilder()
    if (httpOptions.followRedirects) builder.followRedirects(HttpClient.Redirect.ALWAYS)
    else builder.followRedirects(HttpClient.Redirect.NEVER)
    builder.connectTimeout(java.time.Duration.ofMillis(httpOptions.connectTimeout))

    if (httpOptions.sslTrustAll) {
      val trustAllCerts = Array[TrustManager](new X509TrustManager {
        override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
        override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
        override def getAcceptedIssuers: Array[X509Certificate] = Array.empty
      })
      val sslContext = SSLContext.getInstance("TLS")
      sslContext.init(null, trustAllCerts, new SecureRandom())
      builder.sslContext(sslContext)

      val noVerification = new SSLParameters()
      noVerification.setEndpointIdentificationAlgorithm(null)
      builder.sslParameters(noVerification)
    }

    builder.build()
  }

  /**
   * Creates a recurring task to evict unused files.
   */
  private def initScheduler(): Unit = {
    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = try evictUnusedEntries()
        catch { case e: Throwable => e.printStackTrace() }
      },
      evictionCheckInterval,
      evictionCheckInterval,
      TimeUnit.MILLISECONDS)
  }

  /**
   * Downloads or retrieves a file from the cache, returning its local path.
   *
   * @param method HTTP method (e.g., "GET").
   * @param url Remote URL to download from.
   * @param body Optional request body (e.g. for POST).
   * @param headers HTTP request headers.
   * @param readTimeout Per-request read timeout in milliseconds.
   * @return Local file path of the cached file.
   */
  def acquireFor(
      method: String,
      url: String,
      body: Option[String],
      headers: Map[String, String],
      readTimeout: Int): String = synchronized {
    val key = HttpCacheKey(method, url, body, headers)
    val existing = cacheIndex.get(key)
    if (existing != null) {
      existing.usageCount += 1
      logger.debug(s"Existing file for: $method $url headers-size=${headers.size}, usageCount=${existing.usageCount}")
      return existing.localPath
    }
    logger.debug(s"Downloading file for: $method $url headers-size=${headers.size}")
    val localFile = downloadToCache(key, readTimeout)
    val fileSize = localFile.length()
    val newEntry =
      CacheEntry(localPath = localFile.getAbsolutePath, usageCount = 1, lastReleaseTime = 0L, fileSize = fileSize)
    cacheIndex.put(key, newEntry)
    newEntry.localPath
  }

  /**
   * Decrements the usage count for a cached file. If count hits zero, it will be eligible for eviction after
   * idleTimeoutMillis.
   *
   * @param method HTTP method used in the request.
   * @param url Remote URL originally fetched.
   * @param body Optional body used in the request.
   * @param headers Headers that were used.
   */
  def releaseFile(method: String, url: String, body: Option[String], headers: Map[String, String]): Unit =
    synchronized {
      val key = HttpCacheKey(method, url, body, headers)
      val entry = cacheIndex.get(key)
      if (entry == null) {
        logger.warn(s"Could not release, entry not found: $method $url headers-size=${headers.size}")
        return
      }
      if (entry.usageCount > 0) {
        entry.usageCount -= 1
        entry.lastReleaseTime = System.currentTimeMillis()
        logger.debug(s"Released file for: $method $url headers-size=${headers.size}, usageCount=${entry.usageCount}")
      } else {
        logger.warn(s"Released file with usageCount=0: $method $url headers-size=${headers.size}")
      }
    }

  /**
   * Retrieves content from the remote source. Throws an exception if the HTTP status is not 2xx.
   */
  private[datafiles] def downloadToCache(key: HttpCacheKey, readTimeout: Int): File = {
    val uri = URI.create(key.url)
    val reqBuilder = HttpRequest
      .newBuilder(uri)
      .timeout(java.time.Duration.ofMillis(readTimeout))
      .method(key.method.trim.toUpperCase, key.body.map(BodyPublishers.ofString).getOrElse(BodyPublishers.noBody()))
    key.headers.foreach { case (k, v) => reqBuilder.header(k, v) }

    val response = httpClient.send(reqBuilder.build(), HttpResponse.BodyHandlers.ofInputStream())
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      response.body().close()
      throw new RuntimeException(s"HTTP ${key.method} to ${key.url} returned status code ${response.statusCode()}")
    }

    val baseName = Paths.get(uri.getPath).getFileName.toString
    val uniqueName = s"${UUID.randomUUID().toString.take(8)}-$baseName"
    val outFile = new File(cacheDir, uniqueName)

    val inStream = response.body()
    Files.copy(inStream, outFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    inStream.close()

    outFile
  }

  /**
   * Periodically removes entries that have usageCount=0 and passed idleTimeoutMillis since last release.
   */
  private def evictUnusedEntries(): Unit = synchronized {
    val now = System.currentTimeMillis()
    val it = cacheIndex.values().iterator()
    while (it.hasNext) {
      val entry = it.next()
      val idleTime = now - entry.lastReleaseTime
      if (entry.usageCount <= 0 && idleTime > idleTimeoutMillis) {
        it.remove()
        val f = new File(entry.localPath)
        if (f.exists()) f.delete()
      }
    }
  }

  /**
   * Shuts down the cache, cleaning up files and stopping the eviction thread. Any in-use files will also be removed
   * from disk if still present.
   */
  def shutdown(): Unit = {
    scheduler.shutdown()
    httpClient.close()
    val it = cacheIndex.keys().asIterator()
    while (it.hasNext) {
      val key = it.next()
      val entry = cacheIndex.get(key)
      if (entry.usageCount > 0) {
        logger.warn(s"File ${entry.localPath} for url ${key.url} still in use on shutdown")
      }
      val f = new File(entry.localPath)
      if (f.exists()) {
        f.delete()
      }
    }
    cacheDir.delete()
  }

  /**
   * Returns how many entries currently exist in the cache.
   *
   * @return The number of cache entries.
   */
  def getEntryCount: Int = cacheIndex.size()
}

object HttpFileCache {
  private val config: Config = ConfigFactory.load()
  private val cacheDirStr = config.getString("raw.das.data-files.cache-dir")
  private val idleTimeoutMillis = config.getLong("raw.das.data-files.cache-idle-timeout-ms")
  private val evictionCheckMillis = config.getLong("raw.das.data-files.cache-eviction-check-ms")

  def build(httpOptions: HttpConnectionOptions): HttpFileCache = {
    new HttpFileCache(cacheDirStr, idleTimeoutMillis, evictionCheckMillis, httpOptions)
  }
}
