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

package com.rawlabs.das.datafiles

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.protocol.das.v1.query.Qual
import com.rawlabs.protocol.das.v1.tables.{ColumnDefinition, TableDefinition, TableId}

class CsvTable(config: DataFileConfig, sparkSession: SparkSession, httpFileCache: HttpFileCache)
    extends BaseDataFileTable(config, httpFileCache) {

  override def format: String = "csv"

  // Map our custom configuration keys to the corresponding Spark CSV options.
  // Each entry checks if a key exists in our config and, if so, maps it to the Spark option name.
  private val options: Map[String, String] = Map(
    "header" -> "header", // Whether the first line is a header row.
    "delimiter" -> "delimiter", // The character used to separate fields (default: comma).
    "quote" -> "quote", // The character used for quoting strings (default: double quote).
    "escape" -> "escape", // The character used to escape quotes inside quoted fields.
    "multiLine" -> "multiLine", // Whether a single record can span multiple lines.
    "date_format" -> "dateFormat", // Custom date format to parse date columns.
    "timestamp_format" -> "timestampFormat" // Custom timestamp format to parse timestamp columns.
  ).flatMap { case (key, option) =>
    config.options.get(key).map(value => option -> value)
  }

  /**
   * Build the table definition for the CSV.
   */
  override val tableDefinition: TableDefinition = {
    val builder = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName(tableName))
      .setDescription(s"CSV Table reading from $url")

    columns.foreach { case (colName, colType) =>
      builder.addColumns(ColumnDefinition.newBuilder().setName(colName).setType(colType))
    }

    builder.build()
  }

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
      .options(options)
      .csv(resolvedUrl)
  }

}
