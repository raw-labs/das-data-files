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

package com.rawlabs.das.datafiles.multiformat

import com.rawlabs.das.datafiles.api.{DASDataFilesApi, DataFileTableApi}
import com.rawlabs.das.datafiles.csv.CsvTable
import com.rawlabs.das.datafiles.json.JsonTable
import com.rawlabs.das.datafiles.parquet.ParquetTable
import com.rawlabs.das.datafiles.xml.XmlTable
import com.rawlabs.das.sdk.scala.{DASSdk, DASSdkBuilder}
import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSettings}

/**
 * The main plugin class that registers one table per file.
 */
class DASDataFiles(options: Map[String, String]) extends DASDataFilesApi(options) {

  // Build a list of our tables
  val tables: Map[String, DataFileTableApi] = tableConfig.map { config =>
    val format = config.format.getOrElse(
      throw new DASSdkInvalidArgumentException(s"format not specified for table ${config.name}"))
    format match {
      case "csv"     => config.name -> new CsvTable(config, sparkSession, fileCache)
      case "json"    => config.name -> new JsonTable(config, sparkSession, fileCache)
      case "parquet" => config.name -> new ParquetTable(config, sparkSession, fileCache)
      case "xml"     => config.name -> new XmlTable(config, sparkSession, fileCache)
      case other     => throw new DASSdkInvalidArgumentException(s"Unsupported format $other")
    }
  }.toMap
}

/**
 * Builder for the "data-files" DAS type. The engine calls build() with the user-provided config, returning a new
 * DASHttp instance.
 */
class DASDataFilesBuilder extends DASSdkBuilder {

  // This must match your "type" field in the config for the plugin
  override def dasType: String = "datafiles"

  override def build(options: Map[String, String])(implicit settings: DASSettings): DASSdk = {
    new DASDataFiles(options)
  }
}
