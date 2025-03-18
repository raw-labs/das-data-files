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

package com.rawlabs.das.datafiles.parquet

import java.io.File
import java.net.URI

import org.apache.spark.sql.SaveMode
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.datafiles.SparkTestContext
import com.rawlabs.das.datafiles.api.DataFilesTableConfig
import com.rawlabs.das.datafiles.filesystem.DASFileSystem
import com.rawlabs.protocol.das.v1.query.Qual

class ParquetTableTest extends AnyFlatSpec with Matchers with SparkTestContext with BeforeAndAfterAll {

  private var tempDir: File = _
  private val mockFileSystem = mock(classOf[DASFileSystem])

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create a small Parquet file using Spark
    tempDir = File.createTempFile("testData-", ".parquet")
    tempDir.delete()
    tempDir.mkdirs()

    val df = spark.createDataFrame(Seq((1, "Alice"), (2, "Bob"), (3, "Carol"))).toDF("id", "name")

    df.write.mode(SaveMode.Overwrite).parquet(tempDir.getAbsolutePath)

    // Stub the cache call
    when(mockFileSystem.getLocalUrl(ArgumentMatchers.eq("file://mocked/test.parquet")))
      .thenReturn(Right(tempDir.getAbsolutePath))
  }

  "ParquetTable" should "load rows from a Parquet file" in {
    val config = DataFilesTableConfig(
      name = "testParquet",
      uri = new URI("file://mocked/test.parquet"),
      format = Some("parquet"),
      options = Map.empty,
      filesystem = mockFileSystem)

    val table = new ParquetTable(config, spark)
    val result = table.execute(Seq.empty[Qual], Seq.empty[String], Seq.empty, None)

    val rows = Iterator
      .continually(result)
      .takeWhile(_.hasNext)
      .map { x =>
        val row = x.next()
        row.getColumns(0).getName shouldBe "id"
        row.getColumns(1).getName shouldBe "name"
        (row.getColumns(0).getData.getInt.getV, row.getColumns(1).getData.getString.getV)
      }
      .toList

    assert(rows.toSet == Set((1, "Alice"), (2, "Bob"), (3, "Carol")))
  }
}
