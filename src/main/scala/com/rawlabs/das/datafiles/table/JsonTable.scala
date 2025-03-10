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

package com.rawlabs.das.datafiles.table

import com.rawlabs.das.datafiles.{DataFileConfig, HttpFileCache}
import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.protocol.das.v1.query.Qual
import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonTable(config: DataFileConfig, sparkSession: SparkSession, httpFileCache: HttpFileCache)
    extends BaseDataFileTable(config, httpFileCache) {

  override def format: String = "json"

  // Default multiLine to true for standard JSON (pretty printed or array of objects)
  private val multiLine = config.options.getOrElse("multiLine", "true").toBoolean

  // Map our custom configuration keys to the corresponding Spark JSON options.
  private val sparkOptions: Map[String, String] = remapOptions(
    Map(
      "mode" -> "mode", // How to handle corrupt records: PERMISSIVE, DROPMALFORMED, or FAILFAST.
      "date_format" -> "dateFormat", // Custom date format for parsing date values.
      "timestamp_format" -> "timestampFormat", // Custom timestamp format for parsing timestamps.
      "allow_comments" -> "allowComments", // Whether to allow comments in the JSON file.
      "drop_field_if_all_null" -> "dropFieldIfAllNull", // Whether to drop fields that are always null.
      "column_name_of_corrupt_record" -> "columnNameOfCorruptRecord" // Name for field holding corrupt records.
    ))


  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    // We can't easily know row counts without reading the file.
    // Here, just guess or do some sampling logic if you wish:
    DASTable.TableEstimate(expectedNumberOfRows = 10000, avgRowWidthBytes = 100)
  }

  /**
   * Override to read JSON with Spark, parse any relevant options from the `options` map.
   */
  override protected def loadDataFrame(resolvedUrl: String): DataFrame = {
    sparkSession.read
      .option("inferSchema", "true")
      .option("multiLine", multiLine)
      .options(sparkOptions)
      .json(resolvedUrl)
  }
}
