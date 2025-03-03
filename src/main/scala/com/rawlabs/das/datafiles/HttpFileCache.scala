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

import java.io.File
import java.net.URI
import java.net.http.HttpRequest.{BodyPublisher, BodyPublishers}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.UUID
import java.util.concurrent._

import com.typesafe.config.{Config, ConfigFactory}

/**
 * A class that caches files downloaded from HTTP(S) endpoints with various methods, using Java 11+
 * java.net.http.HttpClient.
 *
 * Requires running on Java 11 or newer.
 */
class HttpFileCache(
    cacheDir: File,
    maxCacheBytes: Long,
    highWaterMarkFraction: Double, // e.g. 0.75 => 75%
    lowWaterMarkFraction: Double // e.g. 0.50 => 50%
) {

  // ... existing fields and methods from your code ...
  private case class HttpRequestKey(method: String, url: String, body: Option[String], headers: Map[String, String])

  // A concurrency-safe map of cache keys to CacheEntry
  private val cacheIndex = new ConcurrentHashMap[HttpRequestKey, CacheEntry]()

  private val httpClient: HttpClient =
    HttpClient
      .newBuilder()
      .followRedirects(HttpClient.Redirect.NORMAL)
      .build()

  private val scheduler = Executors.newSingleThreadScheduledExecutor()
  initScheduler()

  // Basic structure to hold local file info
  private case class CacheEntry(
      cacheKey: HttpRequestKey,
      localPath: String,
      fileSize: Long,
      @volatile var lastAccess: Long)

  require(highWaterMarkFraction > lowWaterMarkFraction, "High water mark must be greater than low water mark")

  if (!cacheDir.exists()) cacheDir.mkdirs()

  /**
   * Periodically run eviction
   */
  private def initScheduler(): Unit = {
    val task = new Runnable {
      override def run(): Unit = {
        try evictIfNeeded()
        catch {
          case e: Throwable => e.printStackTrace()
        }
      }
    }
    // check every 60 seconds
    scheduler.scheduleAtFixedRate(task, 60, 60, TimeUnit.SECONDS)
  }

  /**
   * Public method: returns a local File that corresponds to (method, url, body, headers). If it's in cache, re-use it;
   * otherwise fetch from remote.
   */
  def getLocalFileFor(
      method: String,
      remoteUrl: String,
      requestBody: Option[String],
      headers: Map[String, String] = Map.empty): File = synchronized {
    val now = System.currentTimeMillis()
    val cacheKey = HttpRequestKey(method, remoteUrl, requestBody, headers)

    // 1) Check if cached
    val existing = cacheIndex.get(cacheKey)
    if (existing != null) {
      existing.lastAccess = now
      return new File(existing.localPath)
    }

    // 2) Not in cache => fetch
    val localFile = downloadToCache(method, remoteUrl, requestBody, headers)
    val fileSize = localFile.length()

    val entry = CacheEntry(cacheKey, localFile.getAbsolutePath, fileSize, now)
    cacheIndex.put(cacheKey, entry)

    // Evict if needed
    evictIfNeeded()

    localFile
  }

  /**
   * Actually perform the HTTP request & save the response to local file
   */
  private def downloadToCache(
      method: String,
      remoteUrl: String,
      requestBody: Option[String],
      headers: Map[String, String]): File = {
    val uri = URI.create(remoteUrl)

    val reqBuilder = HttpRequest.newBuilder(uri)
    val uppercaseMethod = method.trim.toUpperCase()
    val bodyPublisher: BodyPublisher = requestBody match {
      case Some(body) =>
        BodyPublishers.ofString(body)
      case None =>
        BodyPublishers.noBody()
    }
    reqBuilder.method(uppercaseMethod, bodyPublisher)

    headers.foreach { case (k, v) => reqBuilder.header(k, v) }

    val request = reqBuilder.build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream())
    val status = response.statusCode()

    if (status < 200 || status >= 300) {
      response.body().close()
      throw new RuntimeException(s"HTTP $uppercaseMethod to $remoteUrl returned status code $status")
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
   * Evict LRU if usage > high water mark until usage < low water mark
   */
  private def evictIfNeeded(): Unit = synchronized {
    val usage = currentCacheSize()
    val high = (maxCacheBytes * highWaterMarkFraction).toLong
    if (usage <= high) return

    val low = (maxCacheBytes * lowWaterMarkFraction).toLong

    val sortedEntries = cacheIndex
      .values()
      .toArray(new Array[CacheEntry](0))
      .sortBy(_.lastAccess)

    var currentUsage = usage
    var idx = 0
    while (currentUsage > low && idx < sortedEntries.length) {
      val e = sortedEntries(idx)
      idx += 1

      cacheIndex.remove(e.cacheKey)
      val file = new File(e.localPath)
      if (file.exists()) file.delete()
      currentUsage -= e.fileSize
    }
  }

  /** Sum of file sizes in the cache */
  private def currentCacheSize(): Long = {
    var sum = 0L
    val it = cacheIndex.values().iterator()
    while (it.hasNext) {
      sum += it.next().fileSize
    }
    sum
  }

  /** Shut down background thread. */
  def shutdown(): Unit = {
    scheduler.shutdown()
    // No close needed for HttpClient
  }
}

/**
 * Companion object for HttpFileCache.
 *
 *   - Loads config from Typesafe Config
 *   - Exposes a 'global' lazy val if you want a single default instance
 */
object HttpFileCache {

  // We read from a typical "application.conf" or something in the classpath
  private val config: Config = ConfigFactory.load()

  /**
   * If you want a single global instance for the entire app, define it here. The user can call
   * HttpFileCache.global.getLocalFileFor(...)
   */
  lazy val global: HttpFileCache = {
    // For example, let's read the config with some fallback defaults
    val cacheDirStr = config.getString("raw.das.data-files.cache-dir") // e.g. "/tmp/httpCache"
    val cacheDir = new File(cacheDirStr)

    val maxBytes = config.getBytes("raw.das.data-files.cache-dir") // e.g. "1g"
    val highFrac = config.getDouble("raw.das.data-files.high-watermark") // e.g. 0.75
    val lowFrac = config.getDouble("raw.das.data-files.low-watermark") // e.g. 0.50

    // create the instance
    new HttpFileCache(cacheDir, maxBytes, highFrac, lowFrac)
  }
}
