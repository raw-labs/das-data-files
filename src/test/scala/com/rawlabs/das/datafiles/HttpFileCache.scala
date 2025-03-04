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

import java.io.{File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.util.UUID

import org.mockito.Mockito.{doReturn, spy}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HttpFileCacheSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var tempCacheDir: File = _
  private var cache: HttpFileCache = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempCacheDir = new File(System.getProperty("java.io.tmpdir"), "HttpFileCacheTest-" + UUID.randomUUID())
    tempCacheDir.mkdirs()

    // For a small demonstration, set 1 MB max, high=75%, low=50%
    cache = spy(
      new HttpFileCache(
        cacheDir = tempCacheDir,
        maxCacheBytes = 1024 * 1024,
        highWaterMarkFraction = 0.75,
        lowWaterMarkFraction = 0.5))
  }

  override def afterAll(): Unit = {
    cache.stop()
    tempCacheDir.deleteOnExit()
    super.afterAll()
  }

  behavior of "HttpFileCache"

  it should "return the same file from cache on subsequent calls" in {
    // We'll cheat by forcing "download" to write a small file
    val fakeFile = File.createTempFile("fakeDownload-", ".txt")
    fakeFile.deleteOnExit()
    val out = new FileOutputStream(fakeFile)
    out.write("Hello, world!".getBytes(StandardCharsets.UTF_8))
    out.close()

    // Spy on `downloadToCache` so it doesn't do real network calls
    doReturn(fakeFile)
      .when(cache)
      .downloadToCache(
        method = "GET",
        remoteUrl = "http://example.com/data.csv",
        requestBody = None,
        headers = Map.empty,
        connectionOptions = HttpConnectionOptions(followRedirects = true, 10000, 10000, sslTRustAll = false))

    val f1 = cache.getLocalFileFor(
      method = "GET",
      remoteUrl = "http://example.com/data.csv",
      requestBody = None,
      headers = Map.empty,
      connectionOptions = HttpConnectionOptions(followRedirects = true, 10000, 10000, sslTRustAll = false))

    val f2 = cache.getLocalFileFor(
      method = "GET",
      remoteUrl = "http://example.com/data.csv",
      requestBody = None,
      headers = Map.empty,
      connectionOptions = HttpConnectionOptions(followRedirects = true, 10000, 10000, sslTRustAll = false))

    f1 shouldBe f2 // same file reference
  }

  it should "evict files when above high water mark" in {
    // For demonstration, let's artificially add large files to fill the cache
    // Then call `evictIfNeeded()` and check that some files are removed.
    // We'll do a naive approach here:
    val largeFile1 = new File(tempCacheDir, "large1.tmp")
    val out1 = new FileOutputStream(largeFile1)
    out1.write(Array.fill[Byte](800 * 1024)(1)) // 800KB
    out1.close()

    // Suppose the cache thinks largeFile1 is in the index:
    val key1 = HttpCachetKey("GET", "http://example.com/large1", None, Map.empty) // see below
    val entry1 = HttpCacheEntry(key1, largeFile1.getAbsolutePath, 800 * 1024, System.currentTimeMillis())
    putInCacheIndex(cache, key1, entry1)

    // Another large file
    val largeFile2 = new File(tempCacheDir, "large2.tmp")
    val out2 = new FileOutputStream(largeFile2)
    out2.write(Array.fill[Byte](300 * 1024)(1)) // 300KB
    out2.close()

    val key2 = HttpCachetKey("GET", "http://example.com/large2", None, Map.empty)
    val entry2 = HttpCacheEntry(key2, largeFile2.getAbsolutePath, 300 * 1024, System.currentTimeMillis())
    putInCacheIndex(cache, key2, entry2)

    // Now we call `evictIfNeeded()`. We expect it to remove at least one file so that usage < lowWaterMark.
    cache.evictIfNeeded() // might need reflection or make evictIfNeeded() public for testing

    val entries = getEntriesOfCache(cache)
    assert(entries == 1, s"Expected 1 entry in cache, found $entries")

    assert(!largeFile1.exists() || !largeFile2.exists(), "At least one file should be removed")
  }

//  Helpers to handle private structures:
  def putInCacheIndex(cache: HttpFileCache, key: HttpCachetKey, entry: HttpCacheEntry): Unit = {
    // reflect the `cacheIndex` field (ConcurrentHashMap) and do a put
    val f = classOf[HttpFileCache].getDeclaredField("cacheIndex")
    f.setAccessible(true)
    val map = f.get(cache).asInstanceOf[java.util.concurrent.ConcurrentHashMap[HttpCachetKey, HttpCacheEntry]]
    map.put(key, entry)
  }

  def getEntriesOfCache(cache: HttpFileCache): Int = {
    // reflect the `cacheIndex` field (ConcurrentHashMap) and do a put
    val f = classOf[HttpFileCache].getDeclaredField("cacheIndex")
    f.setAccessible(true)
    val map = f.get(cache).asInstanceOf[java.util.concurrent.ConcurrentHashMap[HttpCachetKey, HttpCacheEntry]]
    map.size()
  }

}
