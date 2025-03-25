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

import org.apache.spark.sql.SparkSession

import com.rawlabs.das.datafiles.api.{BaseDataFileTable, DataFilesTableConfig}
import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.protocol.das.v1.query.Qual

/**
 * A table that reads CSV files.
 */
class CsvTable(config: DataFilesTableConfig, sparkSession: SparkSession)
    extends BaseDataFileTable(config, sparkSession) {

  override val format: String = "csv"

  // Default header to true, as most CSV files have a header row.
  private val header = config.options.getOrElse("header", "true")

  override protected val sparkOptions: Map[String, String] =
    Map("inferSchema" -> "true", "header" -> header) ++
      // Map our custom configuration keys to the corresponding Spark options.
      remapOptions(
        Map(
          "delimiter" -> "delimiter", // The character used to separate fields (default: comma).
          "quote" -> "quote", // The character used for quoting strings (default: double quote).
          "escape" -> "escape", // The character used to escape quotes inside quoted fields.
          "comment" -> "comment", // The character used to indicate a comment line.
          "multiline" -> "multiLine", // Whether a single record can span multiple lines.
          "mode" -> "mode", // How to handle corrupt lines: PERMISSIVE, DROPMALFORMED, or FAILFAST.
          "date_format" -> "dateFormat", // Custom date format to parse date columns.
          "timestamp_format" -> "timestampFormat", // Custom timestamp format to parse timestamp columns.
          "ignore_leading_white_space" -> "ignoreLeadingWhiteSpace", // Ignore leading whitespaces in CSV fields.
          "ignore_trailing_whiteSpace" -> "ignoreTrailingWhiteSpace", // Ignore leading whitespaces in CSV fields.
          "null_value" -> "nullValue", // The string representation of a null value.
          "nan_value" -> "nanValue", // The string representation of a NaN value.
          "positive_inf" -> "positiveInf", // The string representation of a positive infinity value.
          "negative_inf" -> "negativeInf", // The string representation of a negative infinity value.
          "sampling_ratio" -> "samplingRatio", // Fraction of input JSON objects used for schema inferring.
          "column_name_of_corrupt_record" -> "columnNameOfCorruptRecord" // Name for field holding corrupt records.
        ))

  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    // We can't easily know row counts without reading the file, so just guess.
    // Or you could read the file once to see how many lines it has.
    DASTable.TableEstimate(expectedNumberOfRows = 10000, avgRowWidthBytes = 100)
  }
}
