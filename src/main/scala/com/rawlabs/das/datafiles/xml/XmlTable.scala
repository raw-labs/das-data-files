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

package com.rawlabs.das.datafiles.xml

import org.apache.spark.sql.SparkSession

import com.rawlabs.das.datafiles.api.{BaseDataFileTable, DataFilesTableConfig}
import com.rawlabs.das.sdk.DASSdkInvalidArgumentException
import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.protocol.das.v1.query.Qual

/**
 * Table that reads an XML file. Uses Spark-XML (com.databricks.spark.xml).
 */
class XmlTable(config: DataFilesTableConfig, sparkSession: SparkSession)
    extends BaseDataFileTable(config, sparkSession) {

  override val format: String = "xml"

  // The row tag is required for XML tables.
  private val rowTag =
    config.pathOptions.getOrElse(
      "row_tag",
      throw new DASSdkInvalidArgumentException("row_tag is required for xml tables"))

  override protected val sparkOptions: Map[String, String] = {
    Map("rowTag" -> rowTag) ++
    // Map our custom configuration keys to the corresponding Spark options.
    remapOptions(
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
        "timestamp_format" -> "timestampFormat", // Custom timestamp format for parsing timestamp fields.
        "column_name_of_corrupt_record" -> "columnNameOfCorruptRecord" // Name for field holding corrupt records.
      ))
  }

  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    // Again, we can't easily know row counts, so we guess or read once:
    DASTable.TableEstimate(expectedNumberOfRows = 10000, avgRowWidthBytes = 250)
  }

}
