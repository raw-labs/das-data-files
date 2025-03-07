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

import org.mockito.Mockito.{doReturn, spy}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.typesafe.scalalogging.StrictLogging

class HttpFileCacheConcurrencyTest extends AnyFlatSpec with Matchers with StrictLogging {

  "HttpFileCache" should "remain correct under concurrent acquires/releases" in {
    val cache = new HttpFileCache(
      cacheDirStr = System.getProperty("java.io.tmpdir"),
      idleTimeoutMillis = 1000L,
      evictionCheckInterval = 200L,
      HttpConnectionOptions(followRedirects = true, connectTimeout = 5000, sslTrustAll = false))

    // Mock download: Or spy the cache so `downloadToCache` won't do real HTTP
    val key = HttpCacheKey("GET", "http://example.com/test.csv", None, Map.empty)

    // Suppose we manually create a local file for the sake of testing concurrency
    val fakeDownload = File.createTempFile("concurrency-", ".csv")
    val out = new FileOutputStream(fakeDownload)
    out.write("id,name\n1,Alice".getBytes(StandardCharsets.UTF_8))
    out.close()

    // Spy or stub
    val spyCache = spy(cache)
    doReturn(fakeDownload).when(spyCache).downloadToCache(key, 5000)

    val acquireThreadCount = 10
    val threads = (1 to acquireThreadCount).map { _ =>
      new Thread(() => {
        val path = spyCache.acquireFor(key.method, key.url, key.body, key.headers, 5000)
        logger.debug("Acquired: " + path)
        Thread.sleep(scala.util.Random.nextInt(50).toLong) // random pause
        spyCache.releaseFile(key.method, key.url, key.body, key.headers)
      })
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    // At the end, usageCount should be 0 => possible eviction after some time
    Thread.sleep(2000)
    // Check if it’s evicted or not:
    spyCache.getEntryCount shouldBe 0

    spyCache.shutdown()
  }
}
