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

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.protocol.das.v1.query.Qual
import com.rawlabs.protocol.das.v1.tables.{ColumnDefinition, TableDefinition, TableId}

class CsvTable(tableName: String, url: String, options: Map[String, String], sparkSession: SparkSession)
    extends BaseDataFileTable(tableName) {

  override def format: String = "csv"

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
  override protected def loadDataFrame(): DataFrame = {
    // Parse CSV-specific options from the `options` map. For example:
    val hasHeader = options.get("header").forall(_.toBoolean)
    val delimiter = options.getOrElse("delimiter", ",")

    sparkSession.read
      .option("header", hasHeader.toString)
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(url)
  }

}
