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

import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, ScheduledThreadPoolExecutor, TimeUnit}

import com.rawlabs.das.datafiles.filesystem.api.BaseFileSystem
import com.typesafe.scalalogging.StrictLogging

/**
 * Represents a cached entry for a given URL.
 *
 * @param localPath The last local file path that was returned by the filesystem for this URL
 * @param timestampMillis The time (System.currentTimeMillis) when we last downloaded
 */
case class CacheEntry(localPath: String, timestampMillis: Long)

/**
 * A standalone manager that: 1) Keeps a list of available BaseFileSystems 2) Finds a suitable FS for a given URL 3)
 * Caches the resulting local path for a configurable TTL 4) Periodically cleans up old entries and (optionally) deletes
 * the local files
 */
class FileCacheManager(
    fileSystems: Seq[BaseFileSystem], // a list of FSes (S3, GitHub, local, etc.)
    cacheTtlMillis: Int, // how long to consider a cached entry "fresh",
    intervalMillis: Int // how often to clean up the cache
) extends StrictLogging {

  // Thread-safe map: url -> CacheEntry
  private val cacheMap = new ConcurrentHashMap[String, CacheEntry]()

  // Optional background scheduler for automatic cleanup
  private val scheduler: ScheduledExecutorService = new ScheduledThreadPoolExecutor(1)
  private val scheduledCleanup = scheduler.scheduleWithFixedDelay(
    () => {
      try {
        cleanupCache()
      } catch {
        case ex: Throwable =>
          logger.error("Error while cleaning file cache in background:", ex)
      }
    },
    0L,
    intervalMillis,
    TimeUnit.MILLISECONDS)

  // ------------------------------------------------------------------------
  // Public API
  // ------------------------------------------------------------------------

  /**
   * Return a local path for the given URL. If a fresh entry exists in the cache, return that; otherwise ask the correct
   * filesystem for the local file path and store it in the cache.
   */
  def getLocalPathForUrl(url: String): Either[FileSystemError, String] = {
    // 1) Check if we already have a cached entry that is still "fresh"
    val now = System.currentTimeMillis()
    val cached = cacheMap.get(url)
    if (cached != null) {
      val age = now - cached.timestampMillis
      if (age < cacheTtlMillis) {
        // still fresh
        logger.debug(s"Reusing cached file for '$url' -> ${cached.localPath} (age=$age ms)")
        return Right(cached.localPath)
      } else {
        logger.debug(s"Cache entry for '$url' is expired (age=$age ms). Will delete and re-download.")
        deleteFile(url, cached.localPath)
      }
    }

    // 2) Find a filesystem that supports the URL
    val maybeFs = fileSystems.find(_.supportsUrl(url))
    if (maybeFs.isEmpty) {
      logger.warn(s"No filesystem found that supports '$url'")
      return Left(FileSystemError.Unsupported(s"No filesystem supports '$url'"))
    }
    val fs = maybeFs.get

    // 3) Download (or open) the file path from that filesystem
    fs.getLocalUrl(url) match {
      case Left(err) =>
        // We cannot cache an error -> just return it
        Left(err)

      case Right(localPath) =>
        // 4) Store a new CacheEntry in our map
        cacheMap.put(url, CacheEntry(localPath, now))
        logger.debug(s"Caching new entry for '$url' -> $localPath")
        Right(localPath)
    }
  }

  /**
   * Stop the background cleanup thread (if started). This cancels further scheduled tasks.
   */
  private def stopBackgroundCleanup(): Unit = synchronized {
    logger.info("Stopping background cleanup for file cache.")
    scheduledCleanup.cancel(true)
  }

  /**
   * Shut down the underlying scheduler entirely. If you have no other uses for the thread, call this to clean up
   * resources.
   */
  def stop(): Unit = {
    logger.info("Shutting down file cache manager's scheduler.")
    stopBackgroundCleanup()
    scheduler.shutdown()
    clearAll()
  }

  /**
   * Cleans up any cache entries older than TTL, and optionally removes their local files from disk. If you are calling
   * `startBackgroundCleanup()`, this will happen automatically on a schedule.
   */
  private def cleanupCache(): Unit = {
    val now = System.currentTimeMillis()

    // Collect keys to remove
    val iter = cacheMap.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val url = entry.getKey
      val value = entry.getValue
      val age = now - value.timestampMillis

      if (age > cacheTtlMillis) {
        logger.debug(s"Evicting stale cache entry for '$url' (age=$age ms), path=${value.localPath}")
        deleteFile(entry.getKey, entry.getValue.localPath)
        // remove from map
        iter.remove()
      }
    }
  }

  private def deleteFile(originalUrl: String, path: String): Unit = {

    if (originalUrl == path) {
      logger.warn("Not deleting local url: {}", originalUrl)
      return
    }

    val f = new java.io.File(path)
    if (f.exists()) {
      logger.debug(s"Deleting local file: ${f.getAbsolutePath}")
      f.delete()
    }
  }
  // ------------------------------------------------------------------------
  // Additional convenience methods (optional)
  // ------------------------------------------------------------------------

  /**
   * Clears the entire cache map at once. Possibly dangerous if in use.
   */
  private def clearAll(): Unit = {
    logger.info("Clearing all cache entries and deleting all files.")

    val iter = cacheMap.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      deleteFile(entry.getKey, entry.getValue.localPath)
    }
    cacheMap.clear()
  }
}
