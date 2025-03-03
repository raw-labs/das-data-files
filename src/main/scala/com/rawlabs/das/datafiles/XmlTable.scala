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

/**
 * Table that reads an XML file.
 * Uses Spark-XML (com.databricks.spark.xml).
 */
class XmlTable(tableName: String, url: String, options: Map[String, String], spark: SparkSession)
  extends BaseDataFileTable(tableName) {

  override def format: String = "xml"

  /**
   * Build the table definition for the XML file.
   */
  override val tableDefinition: TableDefinition = {
    val builder = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName(tableName))
      .setDescription(s"XML Table reading from $url")

    columns.foreach { case (colName, colType) =>
      builder.addColumns(
        ColumnDefinition.newBuilder().setName(colName).setType(colType)
      )
    }

    builder.build()
  }

  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    // Again, we can't easily know row counts, so we guess or read once:
    DASTable.TableEstimate(expectedNumberOfRows = 10000, avgRowWidthBytes = 250)
  }

  /**
   * Override to read XML using the Databricks Spark XML library.
   * For example, parse "rowTag", "rootTag", "charset", etc. from `options`.
   */
  override protected def loadDataFrame(): DataFrame = {
    // Typically: spark.read.format("xml").option("rowTag", "book").load(...)
    val reader = spark.read.format("xml")

    // Pass all prefixed 'option_' keys or just pass them all.
    options.foreach { case (key, value) =>
      reader.option(key, value)
    }

    // If user didn't specify rowTag, you might default to something:
    if (!options.contains("rowTag")) {
      reader.option("rowTag", "row")
    }

    reader.load(url)
  }
}