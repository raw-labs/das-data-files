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

package com.rawlabs.das.datafiles

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpRequest.{BodyPublisher, BodyPublishers}
import java.net.http.HttpResponse
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import javax.net.ssl.{SSLContext, SSLParameters, TrustManager, X509TrustManager}

// The key
case class HttpCacheKey(method: String, url: String, body: Option[String], headers: Map[String, String])

/**
 * A reference-counted HTTP file cache:
 *
 *   - Acquire a file via 'acquireFileFor', returns a local file, increments usageCount.
 *   - Release it via 'releaseFile', decrements usageCount.
 *   - If usageCount == 0, we wait idleTimeoutMillis from the last release before deleting the file.
 */
class HttpFileCache(
    cacheDir: File,
    idleTimeoutMillis: Long, // e.g. 60_000 for 1 minute
    evictionCheckInterval: Long // how often the background thread checks, e.g. 10_000 ms
) {

  require(idleTimeoutMillis >= 0, "idleTimeoutMillis must be non-negative")
  require(evictionCheckInterval > 0, "evictionCheckInterval must be positive")

  if (!cacheDir.exists()) cacheDir.mkdirs()

  // The cache entry with reference counting
  private case class CacheEntry(
      key: HttpCacheKey,
      localPath: String, // where the file is stored
      var usageCount: Int, // how many clients are using it
      var lastReleaseTime: Long,
      var fileSize: Long)

  // Internal map from key -> CacheEntry
  private val cacheIndex = new ConcurrentHashMap[HttpCacheKey, CacheEntry]()

  // Start a background thread to periodically evict unused files
  private val scheduler = Executors.newSingleThreadScheduledExecutor()
  initScheduler()

  private def initScheduler(): Unit = {
    val task = new Runnable {
      override def run(): Unit = {
        try evictUnusedEntries()
        catch {
          case e: Throwable => e.printStackTrace()
        }
      }
    }
    scheduler.scheduleAtFixedRate(task, evictionCheckInterval, evictionCheckInterval, TimeUnit.MILLISECONDS)
  }

  /**
   * Acquire a file from cache, increment usageCount. If not present, download it from the remote URL and store it in
   * the cache.
   */
  def acquireFor(
      method: String,
      url: String,
      body: Option[String],
      headers: Map[String, String],
      options: HttpConnectionOptions): String = synchronized {
    val key = HttpCacheKey(method, url, body, headers)
    val existing = cacheIndex.get(key)
    if (existing != null) {
      existing.usageCount += 1
      return existing.localPath
    }

    // Not in cache => download
    val localFile = downloadToCache(key, options)
    val fileSize = localFile.length()
    val newEntry = CacheEntry(
      key = key,
      localPath = localFile.getAbsolutePath,
      usageCount = 1, // we are acquiring now
      lastReleaseTime = 0L, // not relevant yet
      fileSize = fileSize)
    cacheIndex.put(key, newEntry)

    newEntry.localPath
  }

  /**
   * Release a file previously acquired, decrement usageCount. If usageCount goes to zero, we record lastReleaseTime and
   * the file will be cleaned up after idleTimeoutMillis if not re-acquired.
   */
  def releaseFile(method: String, url: String, body: Option[String], headers: Map[String, String]): Unit =
    synchronized {
      val key = HttpCacheKey(method, url, body, headers)
      val entry = cacheIndex.get(key)
      if (entry == null) {
        // not found => might be a bug or repeated release
        return
      }
      if (entry.usageCount > 0) {
        entry.usageCount -= 1
        if (entry.usageCount == 0) {
          entry.lastReleaseTime = System.currentTimeMillis()
        }
      }
    }

  /**
   * A simple placeholder for your actual HTTP download logic. You might have timeouts, SSL ignoring, advanced config,
   * etc.
   */
  private[datafiles] def downloadToCache(key: HttpCacheKey, options: HttpConnectionOptions): File = {
    val uri = URI.create(key.url)

    val httpClient = buildHttpClient(options.followRedirects, options.connectTimeout, options.sslTRustAll)

    val reqBuilder = HttpRequest.newBuilder(uri)
    val method = key.method.trim.toUpperCase()
    val bodyPublisher: BodyPublisher = key.body match {
      case Some(body) =>
        BodyPublishers.ofString(body)
      case None =>
        BodyPublishers.noBody()
    }

    reqBuilder.timeout(java.time.Duration.ofMillis(options.readTimeout))

    reqBuilder.method(method, bodyPublisher)

    key.headers.foreach { case (k, v) => reqBuilder.header(k, v) }

    val request = reqBuilder.build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream())
    val status = response.statusCode()

    if (status < 200 || status >= 300) {
      response.body().close()
      throw new RuntimeException(s"HTTP $method to ${key.url} returned status code $status: ${response.body()}")
    }

    // Save to local file
    val baseName = Paths.get(uri.getPath).getFileName.toString
    val uniqueId = UUID.randomUUID().toString.take(8)
    val uniqueName = s"$uniqueId-$baseName"
    val outFile = new File(cacheDir, uniqueName)

    val inStream = response.body()
    Files.copy(inStream, outFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    inStream.close()

    outFile
  }

  /**
   * Helper to build an HttpClient with connect-timeout, SSL trust-all, and redirect handling.
   */
  private def buildHttpClient(followRedirect: Boolean, connectTimeoutMillis: Int, sslTrustAll: Boolean): HttpClient = {
    val builder = HttpClient.newBuilder()

    // Follow redirects if set
    if (followRedirect) builder.followRedirects(HttpClient.Redirect.ALWAYS)
    else builder.followRedirects(HttpClient.Redirect.NEVER)

    // Connect timeout
    builder.connectTimeout(java.time.Duration.ofMillis(connectTimeoutMillis))

    // SSL trust all
    if (sslTrustAll) {
      val trustAllCerts = Array[TrustManager](new X509TrustManager {
        override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
        override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
        override def getAcceptedIssuers: Array[X509Certificate] = Array.empty
      })

      val sslContext = SSLContext.getInstance("TLS")
      sslContext.init(null, trustAllCerts, new SecureRandom())
      builder.sslContext(sslContext)

      // also disable hostname verification
      val noVerification = new SSLParameters()
      noVerification.setEndpointIdentificationAlgorithm(null)
      builder.sslParameters(noVerification)
    }

    builder.build()
  }

  /**
   * The background job that checks for entries with usageCount=0 and (System.currentTimeMillis - lastReleaseTime) >
   * idleTimeoutMillis, then removes them from the cache and deletes the file.
   */
  private def evictUnusedEntries(): Unit = synchronized {
    val now = System.currentTimeMillis()
    val it = cacheIndex.values().iterator()
    while (it.hasNext) {
      val entry = it.next()
      if (entry.usageCount == 0 && (now - entry.lastReleaseTime) > idleTimeoutMillis) {
        // remove from map
        it.remove()
        // delete file
        val f = new File(entry.localPath)
        if (f.exists()) {
          f.delete()
        }
      }
    }
  }

  /**
   * Stop the background eviction thread. Optionally, you might want to evictAll or do a final pass.
   */
  def shutdown(): Unit = {
    scheduler.shutdown()
    // do a final cleanup if desired
    evictUnusedEntries()
  }

  /**
   * For debugging/tests: how many entries are in the cache?
   */
  def getEntryCount: Int = cacheIndex.size()
}

object HttpFileCache {
  private val config: Config = ConfigFactory.load()
  private val cacheDirStr = config.getString("raw.das.data-files.cache-dir")
  private val idleTimeoutMillis = config.getLong("raw.das.data-files.cache-idle-timeout-ms")
  private val evictionCheckMillis = config.getLong("raw.das.data-files.cache-eviction-check-ms")

  lazy val global: HttpFileCache = new HttpFileCache(new File(cacheDirStr), idleTimeoutMillis, evictionCheckMillis)
}