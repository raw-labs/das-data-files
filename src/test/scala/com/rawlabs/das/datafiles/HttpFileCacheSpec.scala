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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class HttpFileCacheSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  private var tempCacheDir: File = _
  private var cache: HttpFileCache = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    // We'll store files in a random temp directory
    tempCacheDir = new File(System.getProperty("java.io.tmpdir"), s"HttpFileCacheTest-${UUID.randomUUID()}")
    tempCacheDir.mkdirs()
  }

  override def afterAll(): Unit = {
    tempCacheDir.deleteOnExit()
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // We'll use a short idle timeout so we can test eviction quickly (e.g. 2 seconds)
    // We'll run eviction checks more frequently, e.g. every 500ms, for demonstration
    cache = spy(
      new HttpFileCache(
        cacheDir = tempCacheDir,
        idleTimeoutMillis = 2000, // 2 seconds idle
        evictionCheckInterval = 500 // check every 0.5s
      ))
  }

  override def afterEach(): Unit = {
    cache.shutdown()
    super.afterEach()
  }

  behavior of "HttpFileCache (reference-counted)"

  it should "return the same file from cache on subsequent acquires" in {
    // We create a small "fake" local file to mimic a downloaded file
    val fakeFile = File.createTempFile("fakeDownload-", ".txt")
    fakeFile.deleteOnExit()
    val out = new FileOutputStream(fakeFile)
    out.write("Hello, world!".getBytes(StandardCharsets.UTF_8))
    out.close()

    // We'll mock the 'downloadFile' method so it doesn't do real network calls
    val mockKey = HttpCacheKey("GET", "http://example.com/data.csv", None, Map.empty)
    val httpOptions =
      HttpConnectionOptions(followRedirects = true, connectTimeout = 10000, readTimeout = 10000, sslTRustAll = false)

    doReturn(fakeFile)
      .when(cache)
      .downloadToCache(mockKey, httpOptions)

    // Acquire the file the first time => usageCount=1
    val file1 = new File(cache.acquireFor(mockKey.method, mockKey.url, mockKey.body, mockKey.headers, httpOptions))
    file1 should not be null

    file1.exists() shouldBe true

    // Acquire again => usageCount=2, same local file
    val file2 = new File(cache.acquireFor(mockKey.method, mockKey.url, mockKey.body, mockKey.headers, httpOptions))
    file2 shouldBe file1
    file2.exists() shouldBe true

    // Cache should have exactly 1 entry
    cache.getEntryCount shouldBe 1

    // Now let's release it once => usageCount goes from 2 -> 1
    cache.releaseFile(mockKey.method, mockKey.url, mockKey.body, mockKey.headers)
    // Another release => usageCount goes from 1 -> 0, sets lastReleaseTime
    cache.releaseFile(mockKey.method, mockKey.url, mockKey.body, mockKey.headers)
    // usageCount=0 means it's "idle" now, but won't remove until idleTimeout passes
  }

  it should "evict the file after idle timeout if usage=0" in {
    // We'll do the same approach of mocking a file
    val fakeFile = File.createTempFile("fake2-", ".txt")
    fakeFile.deleteOnExit()
    val out = new FileOutputStream(fakeFile)
    out.write("Another test content".getBytes(StandardCharsets.UTF_8))
    out.close()

    val mockKey = HttpCacheKey("GET", "http://example.com/other.csv", None, Map.empty)
    val httpOptions =
      HttpConnectionOptions(followRedirects = true, connectTimeout = 10000, readTimeout = 10000, sslTRustAll = false)

    doReturn(fakeFile)
      .when(cache)
      .downloadToCache(mockKey, httpOptions)

    // Acquire => usageCount=1
    val local = new File(cache.acquireFor(mockKey.method, mockKey.url, mockKey.body, mockKey.headers, httpOptions))
    local should not be null
    local.exists() shouldBe true

    // Release => usageCount=0, sets lastReleaseTime
    cache.releaseFile(mockKey.method, mockKey.url, mockKey.body, mockKey.headers)

    // Immediately after release, file still exists
    local.exists() shouldBe true
    cache.getEntryCount shouldBe 1

    // Wait longer than idleTimeout (2s). We'll sleep ~3s to be safe
    Thread.sleep(3000)

    // The background eviction thread should have removed it
    // meaning the map is empty and file is physically deleted
    cache.getEntryCount shouldBe 0
    local.exists() shouldBe false
  }

  it should "keep the file if re-acquired before idle timeout" in {
    val fakeFile = File.createTempFile("fake3-", ".txt")
    fakeFile.deleteOnExit()
    val out = new FileOutputStream(fakeFile)
    out.write("Third test content".getBytes(StandardCharsets.UTF_8))
    out.close()

    val key = HttpCacheKey("GET", "http://example.com/again.csv", None, Map.empty)
    val httpOptions =
      HttpConnectionOptions(followRedirects = true, connectTimeout = 10000, readTimeout = 10000, sslTRustAll = false)

    doReturn(fakeFile)
      .when(cache)
      .downloadToCache(key, httpOptions)

    // Acquire => usageCount=1
    val local = new File(cache.acquireFor(key.method, key.url, key.body, key.headers, httpOptions))
    // Release => usageCount=0
    cache.releaseFile(key.method, key.url, key.body, key.headers)

    // Now, if we re-acquire before the idleTimeout, usageCount=1 again
    Thread.sleep(1000) // less than idleTimeout (2s)
    val local2 = new File(cache.acquireFor(key.method, key.url, key.body, key.headers, httpOptions))
    local2 shouldBe local
    local2.exists() shouldBe true
    // usageCount is now 1

    // Check that after next release, we wait again for idle
    cache.releaseFile(key.method, key.url, key.body, key.headers)
    // usageCount=0

    // Sleep 3s, file should be gone
    Thread.sleep(3000)
    local2.exists() shouldBe false
    cache.getEntryCount shouldBe 0
  }

}
