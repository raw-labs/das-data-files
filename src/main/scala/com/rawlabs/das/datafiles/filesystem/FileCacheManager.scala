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

import java.io.File
import java.net.URI
import java.nio.file.{Files, StandardCopyOption}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent._

import scala.util.control.NonFatal

import com.rawlabs.das.datafiles.filesystem.api.BaseFileSystem
import com.rawlabs.das.datafiles.filesystem.local.LocalFileSystem
import com.typesafe.scalalogging.StrictLogging

/**
 * Represents a cached entry for a given URL.
 */
case class CacheEntry(localPath: String, timestampMillis: Long)

/**
 * The file cache manager. Now responsible for:
 *   - Deciding how/where to place files on disk
 *   - Checking file sizes
 *   - Doing the actual download from the filesystem's InputStream
 */
class FileCacheManager(
    fileSystems: Seq[BaseFileSystem],
    cacheTtlMillis: Long,
    intervalMillis: Long,
    maxDownloadSize: Long, // Moved from BaseFileSystem
    downloadFolder: File // The actual root folder for downloads
) extends StrictLogging {

  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")

  // Cache map: url -> CacheEntry
  private val cacheMap = new ConcurrentHashMap[String, CacheEntry]()

  // A separate map for per-URL lock objects
  private val urlLocks = new ConcurrentHashMap[String, AnyRef]()

  // Executor for periodic cleanup
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
   * Return a local path for the given URL. If a fresh entry exists in the cache, return that; otherwise, we download
   * the file ourselves.
   */
  def getLocalPathForUrl(url: String): Either[FileSystemError, String] = {

    // Retrieve or create a per-URL lock object
    val lockObj = urlLocks.computeIfAbsent(url, _ => new Object())

    lockObj.synchronized {
      try {
        val now = System.currentTimeMillis()
        val cached = cacheMap.get(url)
        if (cached != null) {
          val age = now - cached.timestampMillis
          if (age < cacheTtlMillis) {
            logger.debug(s"Reusing cached file for '$url' -> ${cached.localPath} (age=$age ms)")
            return Right(cached.localPath)
          } else {
            logger.debug(s"Cache entry for '$url' is expired (age=$age ms). Deleting file and re-downloading.")
            deleteFile(cached.localPath)
            cacheMap.remove(url)
          }
        }

        val fs = fileSystems.find(_.supportsUrl(url)) match {
          case Some(fs) => fs
          case None =>
            logger.warn(s"No filesystem found that supports '$url'")
            return Left(FileSystemError.Unsupported(s"No filesystem supports '$url'"))
        }

        // Local filesystem: just return the URL as is
        if (fs.isInstanceOf[LocalFileSystem]) {
          logger.debug(s"Local filesystem found for '$url'")
          return Right(url)
        }

        // Download the file
        downloadFile(fs, url) match {
          case Left(err) => Left(err)
          case Right(localPath) =>
            cacheMap.put(url, CacheEntry(localPath, now))
            Right(localPath)
        }
      } finally {
        // Remove the lock object for this URL
        urlLocks.remove(url)
      }
    }
  }

  /**
   * Stop the background cleanup thread and remove everything from the cache.
   */
  def stop(): Unit = {
    logger.info("Shutting down file cache manager.")
    scheduledCleanup.cancel(true)
    scheduler.shutdown()
    clearAll()
  }

  private def downloadFile(fs: BaseFileSystem, url: String): Either[FileSystemError, String] = {

    // 2) Check file size
    fs.getFileSize(url) match {
      case Left(err) => return Left(err)
      case Right(size) if size > maxDownloadSize =>
        logger.warn(s"File $url is too large ($size bytes), skipping download.")
        return Left(FileSystemError.FileTooLarge(url, size, maxDownloadSize))
      case Right(_) => // OK to proceed
    }

    // 3) download the file
    // Include a small unique ID (e.g., short random or the URL hash) to avoid collisions
    val timestamp = dateTimeFormatter.format(LocalDateTime.now())
    val uniqueId = java.util.UUID.randomUUID().toString.take(8)
    // Extract the original file name from the URL.
    val originalFileName = extractFileName(url)
    val downloadingFileName = s"$timestamp-$uniqueId-$originalFileName"
    val downloadingFile = new File(downloadFolder, downloadingFileName)

    // 4) Actually stream the remote file to local
    logger.debug(s"Downloading file from '$url' to ${downloadingFile.getAbsolutePath}")
    val inputStream = fs.open(url) match {
      case Left(err) =>
        logger.warn(s"Failed to open remote file $url: $err")
        return Left(err)
      case Right(is) => is
    }

    try {
      Files.copy(inputStream, downloadingFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    } catch {
      case NonFatal(e) =>
        downloadingFile.delete()
        throw e
    } finally {
      inputStream.close()
    }

    Right(downloadingFile.getAbsolutePath)
  }

  // A helper to extract the original file name from the URL.
  private def extractFileName(url: String): String = {
    val uri = new URI(url)
    val path = uri.getPath
    val fileName = path.substring(path.lastIndexOf('/') + 1)
    if (fileName.nonEmpty) fileName else "downloadedFile"

  }

  // ------------------------------------------------------------------------
  // Cleanup logic
  // ------------------------------------------------------------------------

  private def cleanupCache(): Unit = {
    val now = System.currentTimeMillis()
    val iter = cacheMap.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val (url, cacheEntry) = (entry.getKey, entry.getValue)
      val age = now - cacheEntry.timestampMillis
      if (age > cacheTtlMillis) {
        logger.debug(s"Evicting stale cache entry for '$url' (age=$age ms), path=${cacheEntry.localPath}")
        deleteFile(cacheEntry.localPath)
        iter.remove()
      }
    }
  }

  private def deleteFile(path: String): Unit = {
    val f = new File(path)
    if (f.exists()) {
      logger.debug(s"Deleting local file: ${f.getAbsolutePath}")
      f.delete()
    }
  }

  private def clearAll(): Unit = {
    logger.info("Clearing all cache entries and deleting all files.")
    val iter = cacheMap.entrySet().iterator()
    while (iter.hasNext) {
      val e = iter.next()
      deleteFile(e.getValue.localPath)
    }
    cacheMap.clear()
  }
}
