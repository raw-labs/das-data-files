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

package com.rawlabs.das.datafiles.csv

import com.rawlabs.das.datafiles.api.{DataFileTableApi, DataFilesTableConfig}
import com.rawlabs.das.datafiles.filesystem.DataFilesCache
import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.protocol.das.v1.query.Qual
import org.apache.spark.sql.{DataFrame, SparkSession}

class CsvTable(config: DataFilesTableConfig, sparkSession: SparkSession, fileCache: DataFilesCache)
    extends DataFileTableApi(config, fileCache) {

  override def format: String = "csv"

  // Default header to true, as most CSV files have a header row.
  private val header = config.options.getOrElse("header", "true").toBoolean

  // Map our custom configuration keys to the corresponding Spark CSV options.
  // Each entry checks if a key exists in our config and, if so, maps it to the Spark option name.
  private val sparkOptions: Map[String, String] = remapOptions(
    Map(
      "delimiter" -> "delimiter", // The character used to separate fields (default: comma).
      "quote" -> "quote", // The character used for quoting strings (default: double quote).
      "escape" -> "escape", // The character used to escape quotes inside quoted fields.
      "multiline" -> "multiLine", // Whether a single record can span multiple lines.
      "mode" -> "mode", // How to handle corrupt lines: PERMISSIVE, DROPMALFORMED, or FAILFAST.
      "date_format" -> "dateFormat", // Custom date format to parse date columns.
      "timestamp_format" -> "timestampFormat" // Custom timestamp format to parse timestamp columns.
    ))

  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    // We can't easily know row counts without reading the file, so just guess.
    // Or you could read the file once to see how many lines it has.
    DASTable.TableEstimate(expectedNumberOfRows = 10000, avgRowWidthBytes = 100)
  }

  /**
   * Override to read CSV with Spark, parse relevant CSV options from `options`.
   */
  override protected def loadDataFrame(resolvedUrl: String): DataFrame = {
    sparkSession.read
      .option("inferSchema", "true")
      .option("header", header)
      .options(sparkOptions)
      .csv(resolvedUrl)
  }

}
