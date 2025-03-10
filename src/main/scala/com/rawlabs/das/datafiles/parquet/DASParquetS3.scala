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

package com.rawlabs.das.datafiles.parquet

import com.rawlabs.das.datafiles.{BaseDASDataFiles, BaseDataFileTable}
import com.rawlabs.das.sdk.scala.{DASSdk, DASSdkBuilder}
import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSettings}

/**
 * The main plugin class that registers one table per file.
 */
class DASParquetS3(options: Map[String, String]) extends BaseDASDataFiles(options) {

  // Build a list of our tables
  val tables: Map[String, BaseDataFileTable] = dasOptions.tableConfigs.map { config =>
    if (!config.url.startsWith("s3://")) {
      throw new DASSdkInvalidArgumentException(s"Unsupported URL ${config.url}")
    }
    config.name -> new ParquetTable(config, sparkSession, hppFileCache)
  }.toMap

}

/**
 * Builder for the "s3-csv" DAS type. The engine calls build() with user-provided config, returning a new DasS3Csv
 * instance.
 */
class DASParquetS3Builder extends DASSdkBuilder {

  // This must match your "type" field in the config for the plugin
  override def dasType: String = "s3-parquet"

  override def build(options: Map[String, String])(implicit settings: DASSettings): DASSdk = {
    new DASParquetS3(options)
  }
}
