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

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import com.rawlabs.das.datafiles.api.DataFilesTableConfig
import com.rawlabs.das.datafiles.filesystem.FileCacheManager
import com.rawlabs.protocol.das.v1.query.Qual

class ParquetTableTest
    extends AnyFlatSpec
    with Matchers
    with SparkTestContext
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private var tempDirSingle: File = _
  private var tempDirMerged: File = _
  private val mockCacheManager = mock(classOf[FileCacheManager])

  override def beforeAll(): Unit = {
    super.beforeAll()

    //
    // 1) Create a single Parquet file for the "single-file" test
    //
    tempDirSingle = File.createTempFile("testData-single-", ".parquet")
    tempDirSingle.delete()
    tempDirSingle.mkdirs()

    val dfSingle = spark
      .createDataFrame(Seq((1, "Alice"), (2, "Bob"), (3, "Carol")))
      .toDF("id", "name")

    dfSingle.write.mode(SaveMode.Overwrite).parquet(tempDirSingle.getAbsolutePath)

    // We'll mock the cache manager to point "file://mocked/testSingle.parquet" to that directory
    when(mockCacheManager.getLocalPathForUrl("file://mocked/testSingle.parquet"))
      .thenReturn(Right(tempDirSingle.getAbsolutePath))

    //
    // 2) Create a "merged" Parquet folder with two subfolders, each different schema
    //
    tempDirMerged = File.createTempFile("testData-merged-", "")
    tempDirMerged.delete()
    tempDirMerged.mkdirs()

    // Subfolder 1 => (id, name)
    val part1 = new File(tempDirMerged, "part1")
    part1.mkdirs()
    val dfPart1 = spark.createDataFrame(Seq((100, "Alpha"), (101, "Beta"))).toDF("id", "name")
    dfPart1.write.mode(SaveMode.Overwrite).parquet(part1.getAbsolutePath)

    // Subfolder 2 => (id, age)
    val part2 = new File(tempDirMerged, "part2")
    part2.mkdirs()
    val dfPart2 = spark.createDataFrame(Seq((102, 33), (103, 44))).toDF("id", "age")
    dfPart2.write.mode(SaveMode.Overwrite).parquet(part2.getAbsolutePath)

    // We'll mock the cache manager for "file://mocked/merged.parquet"
    when(mockCacheManager.getLocalPathForUrl("file://mocked/merged.parquet"))
      .thenReturn(Right(tempDirMerged.getAbsolutePath))
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  behavior of "ParquetTable"

  // --------------------------------------------------------------------------
  // Single-file test
  // --------------------------------------------------------------------------
  it should "load rows from a single Parquet file" in {
    val config = DataFilesTableConfig(
      name = "testParquetSingle",
      uri = new URI("file://mocked/testSingle.parquet"),
      format = Some("parquet"),
      options = Map.empty,
      fileCacheManager = mockCacheManager)

    val table = new ParquetTable(config, spark)
    val result = table.execute(Seq.empty[Qual], Seq.empty[String], Seq.empty, None)

    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.map { row =>
      val id = row.getColumns(0).getData.getInt.getV
      val name = row.getColumns(1).getData.getString.getV
      (id, name)
    }.toSet shouldBe Set((1, "Alice"), (2, "Bob"), (3, "Carol"))
  }

  // --------------------------------------------------------------------------
  // MergeSchema test with multiple files
  //  set "recursive_file_lookup" -> "true" so Spark actually descends into
  //  the sub-folders. We also set "merge_schema" -> "true" so it
  // merges columns from each parquet file.
  // --------------------------------------------------------------------------
  it should "support mergeSchema across multiple Parquet files" in {
    // We'll point to the "merged" folder, containing two subfolders
    // and set merge_schema => "true"
    val config = DataFilesTableConfig(
      name = "testParquetMerged",
      uri = new URI("file://mocked/merged.parquet"),
      format = Some("parquet"),
      options = Map(
        "merge_schema" -> "true",
        "recursive_file_lookup" -> "true" // needed because top-level has subfolders
      ),
      fileCacheManager = mockCacheManager)

    val table = new ParquetTable(config, spark)
    val result = table.execute(Nil, Nil, Nil, None)

    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 4 // 2 from part1, 2 from part2

    // Let's confirm the schema in the TableDefinition has columns: "id", "name", "age"
    // (Spark merges them)
    val defn = table.tableDefinition
    val colNames = defn.getColumnsList
    colNames.size() shouldBe 3 // id, name, age
    colNames.get(0).getName shouldBe "id"
    colNames.get(1).getName shouldBe "name"
    colNames.get(2).getName shouldBe "age"

    // Convert the rows to a (id, nameOpt, ageOpt) triple, so we can see who has which columns:
    val rowTuples = rows.map { r =>
      val idVal = r.getColumns(0).getData.getInt.getV
      val nameVal = r.getColumns(1).getData
      val ageVal = r.getColumns(2).getData

      val maybeName = if (nameVal.hasNull) None else Some(nameVal.getString.getV)
      val maybeAge = if (ageVal.hasNull) None else Some(ageVal.getInt.getV)
      (idVal, maybeName, maybeAge)
    }

    // We expect:
    // part1 => (100, "Alpha", None), (101, "Beta", None)
    // part2 => (102, None, 33), (103, None, 44)
    rowTuples.toSet shouldBe Set(
      (100, Some("Alpha"), None),
      (101, Some("Beta"), None),
      (102, None, Some(33)),
      (103, None, Some(44)))
  }

  it should "enable recursiveFileLookup" in {
    val config = DataFilesTableConfig(
      name = "testParquetRecursive",
      uri = new URI("file://mocked/testSingle.parquet"),
      format = Some("parquet"),
      options = Map("recursive_file_lookup" -> "true"),
      fileCacheManager = mockCacheManager)
    val table = new ParquetTable(config, spark)
    val result = table.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 3
  }

  it should "use path_glob_filter" in {
    val config = DataFilesTableConfig(
      name = "testParquetGlob",
      uri = new URI("file://mocked/testSingle.parquet"),
      format = Some("parquet"),
      options = Map("path_glob_filter" -> "*.parquet"),
      fileCacheManager = mockCacheManager)
    val table = new ParquetTable(config, spark)
    val result = table.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 3
  }
}
