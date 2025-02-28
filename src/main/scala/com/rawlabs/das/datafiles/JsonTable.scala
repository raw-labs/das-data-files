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

import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.protocol.das.v1.query.Qual
import com.rawlabs.protocol.das.v1.tables.{ColumnDefinition, TableDefinition, TableId}
import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonTable(tableName: String, url: String, options: Map[String, String], sparkSession: SparkSession)
  extends BaseDataFileTable(tableName) {

  override def format: String = "json"

  /**
   * Build the table definition for the JSON file.
   */
  override val tableDefinition: TableDefinition = {
    val builder = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName(tableName))
      .setDescription(s"JSON Table reading from $url")

    columns.foreach { case (colName, colType) =>
      builder.addColumns(ColumnDefinition.newBuilder().setName(colName).setType(colType))
    }

    builder.build()
  }

  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    // We can't easily know row counts without reading the file.
    // Here, just guess or do some sampling logic if you wish:
    DASTable.TableEstimate(expectedNumberOfRows = 10000, avgRowWidthBytes = 100)
  }

  /**
   * Override to read JSON with Spark, parse any relevant options from the `options` map.
   */
  override protected def loadDataFrame(): DataFrame = {
    // For example, we might want multiLine option if reading multi-line JSON:
    val multiLine = options.get("multiLine").exists(_.toBoolean)

    sparkSession.read
      .option("multiLine", multiLine.toString)
      .option("inferSchema", "true")
      .json(url)
  }
}
