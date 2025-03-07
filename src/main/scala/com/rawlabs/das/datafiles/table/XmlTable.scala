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

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.rawlabs.das.datafiles.{DataFileConfig, HttpFileCache}
import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.protocol.das.v1.query.Qual
import com.rawlabs.protocol.das.v1.tables.{ColumnDefinition, TableDefinition, TableId}

/**
 * Table that reads an XML file. Uses Spark-XML (com.databricks.spark.xml).
 */
class XmlTable(config: DataFileConfig, sparkSession: SparkSession, httpFileCache: HttpFileCache)
    extends BaseDataFileTable(config, httpFileCache) {

  override def format: String = "xml"

  // default row tag is "row"
  private val rowTag = config.options.getOrElse("row_tag", "row")
  // Map our custom configuration keys to the corresponding Spark XML options.
  private val sparkOptions: Map[String, String] = remapOptions(
    Map(
      "root_tag" -> "rootTag", // The tag for the root element in the XML document.
      "attribute_prefix" -> "attributePrefix", // Prefix to add to XML attribute names.
      "values_tag" -> "valueTag", // Tag used to represent the element's text value when it has attributes.
      "ignore_surrounding_spaces" -> "ignoreSurroundingSpaces", // Whether to trim white space around element values.
      "charset" -> "charset", // Character encoding of the XML file (default: UTF-8).
      "treat_empty_values_as_nulls" -> "treatEmptyValuesAsNulls", // Whether to treat empty string values as null.
      "sampling_ratio" -> "samplingRatio", // Ratio of rows to use for schema inference (between 0 and 1).
      "mode" -> "mode", // Error handling mode: PERMISSIVE, DROPMALFORMED, or FAILFAST.
      "date_format" -> "dateFormat", // Custom date format for parsing date fields.
      "timestamp_format" -> "timestampFormat" // Custom timestamp format for parsing timestamp fields.
    ))

  /**
   * Build the table definition for the XML file.
   */
  override val tableDefinition: TableDefinition = {
    val builder = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName(tableName))
      .setDescription(s"XML Table reading from $url")

    columns.foreach { case (colName, colType) =>
      builder.addColumns(ColumnDefinition.newBuilder().setName(colName).setType(colType))
    }

    builder.build()
  }

  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    // Again, we can't easily know row counts, so we guess or read once:
    DASTable.TableEstimate(expectedNumberOfRows = 10000, avgRowWidthBytes = 250)
  }

  /**
   * Override to read XML using the Databricks Spark XML library. For example, parse "rowTag", "rootTag", "charset",
   * etc. from `options`.
   */
  override protected def loadDataFrame(resolvedUrl: String): DataFrame = {
    // Typically: spark.read.format("xml").option("rowTag", "book").load(...)
    sparkSession.read
      .option("inferSchema", "true")
      .option("rowTag", rowTag)
      .format("xml")
      .options(sparkOptions)
      .load(resolvedUrl)
  }
}
