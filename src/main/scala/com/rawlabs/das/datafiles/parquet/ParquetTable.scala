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

import com.rawlabs.das.datafiles.api.{BaseDataFileTable, DataFilesTableConfig}
import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.protocol.das.v1.query.Qual
import org.apache.spark.sql.SparkSession

/**
 * Table that reads a Parquet file.
 */
class ParquetTable(config: DataFilesTableConfig, sparkSession: SparkSession)
    extends BaseDataFileTable(config, sparkSession) {

  override val format: String = "parquet"

  // Map our custom configuration keys to the corresponding Spark options.
  override protected val sparkOptions: Map[String, String] = remapOptions(
    Map(
      "merge_schema" -> "mergeSchema", // Whether to merge schemas from different files when reading from a directory.
      "recursive_file_lookup" -> "recursiveFileLookup", // Whether to recursively search subdirectories for Parquet files.
      "path_glob_filter" -> "pathGlobFilter" // Glob pattern to filter which files to read.
    ))

  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    // Parquet has metadata that might let you guess row count or compression ratio,
    // but here we just do a rough guess:
    DASTable.TableEstimate(expectedNumberOfRows = 10000, avgRowWidthBytes = 200)
  }

}
