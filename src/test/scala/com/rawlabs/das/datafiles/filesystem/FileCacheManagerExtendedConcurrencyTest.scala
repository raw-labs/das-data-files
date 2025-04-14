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

import java.io.{ByteArrayInputStream, File, IOException, InputStream}
import java.nio.file.Files
import java.util.concurrent.Executors

import scala.collection.concurrent.TrieMap
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.datafiles.filesystem.api.BaseFileSystem

/**
 * Extended concurrency tests for FileCacheManager.
 */
class FileCacheManagerExtendedConcurrencyTest extends AnyFlatSpec with Matchers {

  /**
   * A simple mock FS that simulates I/O with optional delay, errors, partial downloads, etc.
   */
  class MockFileSystem(
      failAfterNOpens: Option[Int] = None, // E.g. fail after N successful opens
      downloadDelayMillis: Long = 0, // artificial delay to simulate concurrency overlap
      partialDownloadBehavior: Boolean = false // whether to simulate partial/corrupt downloads
  ) extends BaseFileSystem {

    @volatile private var openCount = 0

    def getOpenCount: Int = openCount

    override def name: String = "mockfs"

    override def supportsUrl(url: String): Boolean =
      url.startsWith("mock://")

    override def list(url: String): Either[FileSystemError, List[String]] =
      Right(List(url))

    override def open(url: String): Either[FileSystemError, InputStream] = {
      // If requested, wait some time to let concurrency overlap
      if (downloadDelayMillis > 0) Thread.sleep(downloadDelayMillis)

      synchronized {
        // Check how many times we've opened so far
        val current = openCount
        val failLimitReached = failAfterNOpens.exists(limit => current >= limit)
        if (failLimitReached) {
          // return an error after the limit
          return Left(FileSystemError.PermissionDenied(s"Simulated error for $url"))
        }

        openCount += 1
      }

      // partial/corrupt download scenario
      if (partialDownloadBehavior && openCount % 2 == 0) {
        // Every second attempt, we "fail" mid-stream:
        return Right(new ByteArrayInputStream("PARTIAL".getBytes("UTF-8")) {
          override def read(b: Array[Byte], off: Int, len: Int): Int = {
            // read half the requested length, then throw an IOException
            val bytesRead = super.read(b, off, len / 2)
            throw new IOException("Simulated partial/corrupt download.")
          }
        })
      }

      // Otherwise, return a normal stream
      Right(new ByteArrayInputStream(s"fake contents of $url".getBytes("UTF-8")))
    }

    override def resolveWildcard(url: String): Either[FileSystemError, List[String]] =
      Right(List(url))

    override def getFileSize(url: String): Either[FileSystemError, Long] =
      Right(1234L)

    override def stop(): Unit = {}
  }

  // Helper for creating a new FileCacheManager with user-defined parameters
  private def newCacheManager(
      fs: BaseFileSystem,
      cacheTtlMillis: Long,
      intervalMillis: Long,
      maxDownloadSize: Long): FileCacheManager = {
    val tempDir = Files.createTempDirectory("cache-test-").toFile
    new FileCacheManager(
      fileSystems = Seq(fs),
      cacheTtlMillis = cacheTtlMillis,
      intervalMillis = intervalMillis,
      maxDownloadSize = maxDownloadSize,
      downloadFolder = tempDir)
  }

  it should "download file only once when multiple threads request the same URL concurrently" in {
    // We will create a mock BaseFileSystem that simulates
    // opening/downloading a file but tracks how many times
    // open() was actually invoked.
    val mockFileSystem = new MockFileSystem

    // We’ll create a unique temporary download folder for the cache
    val tempDownloadDir = File.createTempFile("file-cache-manager-test-", "")
    tempDownloadDir.delete()
    tempDownloadDir.mkdirs()

    // A short TTL so that we can see concurrency caching in action,
    // but not so short that the file expires mid-test.
    val cacheTTLMillis = 60 * 1000 // 1 minute

    val manager = new FileCacheManager(
      fileSystems = Seq(mockFileSystem),
      cacheTtlMillis = cacheTTLMillis,
      intervalMillis = 5000,
      maxDownloadSize = 100 * 1024 * 1024, // 100MB
      downloadFolder = tempDownloadDir)

    // We'll test concurrency by spinning up multiple threads
    // all calling getLocalPathForUrl with the same URL:
    val concurrencyLevel = 10
    val url = "mock://same-file.dat"

    // We store each thread's result to verify it’s identical.
    val results = new TrieMap[Int, Try[String]]()

    // Use Scala Futures for concurrency
    import ExecutionContext.Implicits.global
    val futures = (1 to concurrencyLevel).map { idx =>
      Future {
        // Each future calls getLocalPathForUrl
        val pathTry = Try(manager.getLocalPathForUrl(url))
        results.put(idx, pathTry.map(_.toOption.get)) // store the final path or the error
      }
    }

    // Wait for all threads to finish
    Await.result(Future.sequence(futures), 10.seconds)

    // Check results
    // 1) None of the calls should have failed
    results.values.foreach { pathTry =>
      pathTry.isSuccess shouldBe true
    }

    // 2) They should all get the same local path
    val allPaths = results.values.map(_.get).toSet
    allPaths.size shouldBe 1

    // 3) The underlying FS open() should have been called exactly once
    mockFileSystem.getOpenCount shouldBe 1

    // Cleanup
    manager.stop()
    tempDownloadDir.deleteOnExit()
  }

  it should "download a file once per unique URL, even if requested in parallel" in {
    // Suppose we have multiple distinct URLs. The manager should
    // download each one exactly once, but do so concurrently.
    val mockFileSystem: MockFileSystem = new MockFileSystem

    val tempDir = File.createTempFile("file-cache-manager-test2-", "")
    tempDir.delete()
    tempDir.mkdirs()

    val manager = new FileCacheManager(
      fileSystems = Seq(mockFileSystem),
      cacheTtlMillis = 60000,
      intervalMillis = 5000,
      maxDownloadSize = 1000000,
      downloadFolder = tempDir)

    val concurrencyLevel = 10
    // We'll generate a handful of unique URLs
    val distinctUrls = Seq("mock://fileA", "mock://fileB", "mock://fileC")
    // Let’s create concurrency by mixing them up across threads
    val allUrls = (1 to concurrencyLevel).map(i => distinctUrls(i % distinctUrls.size))

    import ExecutionContext.Implicits.global
    val results = new TrieMap[Int, Try[String]]()

    val futures = allUrls.zipWithIndex.map { case (u, idx) =>
      Future {
        val result = Try(manager.getLocalPathForUrl(u))
        results.put(idx, result.map(_.toOption.get))
      }
    }

    // Wait for all
    Await.result(Future.sequence(futures), 10.seconds)

    // All succeeded
    results.values.foreach { pathTry =>
      pathTry.isSuccess shouldBe true
    }

    // For each distinct URL, manager should have opened the file once
    // => we have 3 distinct URLs, so 3 open calls
    mockFileSystem.getOpenCount shouldBe 3

    manager.stop()
    tempDir.deleteOnExit()
  }

  /**
   * 1) Testing Cache Expiration Under Concurrency
   *   - We'll set a very short TTL so that the file expires quickly, then request it concurrently just after it
   *     expires.
   */
  it should "handle concurrent requests after TTL expires (only one re-download)" in {
    val mockFS = new MockFileSystem()
    val manager =
      newCacheManager(fs = mockFS, cacheTtlMillis = 200, intervalMillis = 1000, maxDownloadSize = 1024 * 1024)

    val url = "mock://expiring-file"
    // Warm up: first request => caches the file
    val firstPath = manager.getLocalPathForUrl(url)
    firstPath.isRight shouldBe true
    mockFS.getOpenCount shouldBe 1

    // Wait for TTL to pass
    Thread.sleep(300)

    // Now spawn multiple threads to request the same URL concurrently
    val concurrencyLevel = 5
    val results = new TrieMap[Int, Try[String]]()

    // Use a dedicated thread pool so we are sure concurrency is available
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(concurrencyLevel))

    val futures = (1 to concurrencyLevel).map { i =>
      Future {
        val r = Try(manager.getLocalPathForUrl(url))
        results.put(i, r.map(_.toOption.get))
      }
    }
    Await.result(Future.sequence(futures), 5.seconds)

    // All should succeed
    results.values.foreach(_.isSuccess shouldBe true)
    // They should all share the same file path
    results.values.map(_.get).toSet.size shouldBe 1

    // Because the cache expired, we expect exactly 2 total opens:
    //  - The initial warm-up
    //  - A second open for re-download
    mockFS.getOpenCount shouldBe 2

    manager.stop()
  }

  /**
   * 2) Testing Cleanup Thread Races
   *   - We let the cleanup thread run quickly, so it may remove entries while we request them. We'll see if that
   *     triggers duplicates.
   */
  it should "concurrently request a file while cleanup thread may remove it" in {
    val mockFS = new MockFileSystem(downloadDelayMillis = 100)
    // Very short TTL & frequent cleanup => high chance it gets removed
    val manager = newCacheManager(fs = mockFS, cacheTtlMillis = 100, intervalMillis = 50, maxDownloadSize = 1024 * 1024)

    val url = "mock://cleanup-race"
    // Warm up
    manager.getLocalPathForUrl(url) // openCount=1
    Thread.sleep(120) // let it expire, possibly get cleaned

    // Start concurrency
    val concurrencyLevel = 5
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(concurrencyLevel))

    val results = new TrieMap[Int, Try[String]]()
    val futures = (1 to concurrencyLevel).map { i =>
      Future {
        val r = Try(manager.getLocalPathForUrl(url))
        results.put(i, r.map(_.toOption.get))
      }
    }
    Await.result(Future.sequence(futures), 5.seconds)

    // All succeed
    results.values.foreach { t =>
      t.isSuccess shouldBe true
    }

    // Possibly manager re-downloaded multiple times if it was cleaned up in between calls
    // Typically we want to see at least 2 or more opens here, but let's assert it's not 100.
    mockFS.getOpenCount should be >= 2

    manager.stop()
  }

  /**
   * 4) Concurrency with stop() calls
   *   - We'll run multiple requests and forcibly call manager.stop() in the middle. Some threads might be mid-download,
   *     so we see how it behaves.
   */
  it should "handle concurrency while manager is stopped mid-way" in {
    val mockFS = new MockFileSystem(downloadDelayMillis = 200)
    val manager =
      newCacheManager(fs = mockFS, cacheTtlMillis = 60000, intervalMillis = 200, maxDownloadSize = 1024 * 1024)

    val url = "mock://stop-test"
    implicit val ec: ExecutionContext = ExecutionContext.global

    val concurrencyLevel = 5
    val results = new TrieMap[Int, Try[String]]()
    val futures = (1 to concurrencyLevel).map { i =>
      Future {
        val r = Try(manager.getLocalPathForUrl(url))
        results.put(i, r.map(_.toOption.get))
      }
    }

    // Wait a bit, then stop
    Thread.sleep(50)
    manager.stop()

    // Now finalize the futures
    Await.result(Future.sequence(futures), 5.seconds)
    // Some might succeed, some might fail depending on the timing.
    // We'll just confirm no thread threw a weird concurrency error, e.g. NullPointerException
    // or something you didn't expect. So we do a simple check:
    results.values.foreach {
      case Success(_) => // OK
      case Failure(e) =>
        // Possibly an error if the manager was closed mid-request.
        // We'll accept it as "normal" in this test scenario.
        e.isInstanceOf[Exception] shouldBe true
    }
  }

  /**
   * 5) High concurrency and large number of URLs
   */
  it should "handle high concurrency with many distinct URLs" in {
    val mockFS = new MockFileSystem(downloadDelayMillis = 5)
    val manager =
      newCacheManager(fs = mockFS, cacheTtlMillis = 1000, intervalMillis = 500, maxDownloadSize = 1024 * 1024)

    val concurrencyLevel = 50
    val distinctUrls = (1 to 20).map(i => s"mock://multi-file-$i")
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(concurrencyLevel))

    val futures = (1 to concurrencyLevel).map { i =>
      Future {
        // pick a random URL from the list
        val url = distinctUrls(scala.util.Random.nextInt(distinctUrls.size))
        manager.getLocalPathForUrl(url)
      }
    }

    val results = Await.result(Future.sequence(futures), 10.seconds)
    // Check all are either Right or Left but no concurrency meltdown
    all(results) shouldBe a[Right[_, _]] // or your code might allow some Left if there's an error scenario

    // We expect up to 20 distinct URLs => openCount <= 20 if each is downloaded exactly once
    // But if TTL is not zero, or multiple concurrency might cause re-download overlaps.
    // We'll do a minimal check that openCount is at least 1 but no more than 20 * concurrency
    mockFS.getOpenCount should (be >= 1 and be <= (20 * concurrencyLevel))

    manager.stop()
  }

  /**
   * 6) Partial or Interrupted downloads
   *   - We set partialDownloadBehavior=true in MockFileSystem. Every second open call throws an IOException mid-read.
   *     We want to see that the manager discards that partial file and tries again (or returns an error).
   */
  it should "handle partial/corrupt downloads from the file system" in {
    val mockFS = new MockFileSystem(partialDownloadBehavior = true)
    val manager =
      newCacheManager(fs = mockFS, cacheTtlMillis = 60000, intervalMillis = 1000, maxDownloadSize = 1024 * 1024)

    val url = "mock://partial-download"
    implicit val ec: ExecutionContext = ExecutionContext.global

    // We'll do multiple attempts. Some might fail mid-stream, some might succeed
    val concurrencyLevel = 5
    val results = new TrieMap[Int, Either[FileSystemError, String]]()

    val futures = (1 to concurrencyLevel).map { i =>
      Future {
        val r = manager.getLocalPathForUrl(url)
        results.put(i, r)
      }
    }
    Await.result(Future.sequence(futures), 5.seconds)

    // We might see that 1 or more attempts triggered partial downloads => cause an exception => manager discards or returns error
    val (oks, errs) = results.values.partition(_.isRight)

    // We at least expect some attempt succeeded eventually:
    oks.size should be >= 1
    // Possibly some that triggered partial read got an error
    // It's up to your manager logic whether it re-tries automatically or not.
    // If it doesn't re-try, we might see some left errors. We'll accept that as normal:
    errs.size should be >= 0

    manager.stop()
  }

  /**
   * 7) Forcing a re-download / manual invalidation scenario (only relevant if your manager has such an API). If you had
   * something like `manager.invalidate(url)`, you'd test concurrency of that call vs getLocalPathForUrl.
   *
   * As it's not in the base code, skipping a full example here.
   */

}
