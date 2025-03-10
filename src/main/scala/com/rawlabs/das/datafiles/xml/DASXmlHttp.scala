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

package com.rawlabs.das.datafiles.xml

import com.rawlabs.das.datafiles.{BaseDASDataFiles, BaseDataFileTable}
import com.rawlabs.das.sdk.scala.{DASSdk, DASSdkBuilder}
import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSettings}

class DASXmlHttp(options: Map[String, String]) extends BaseDASDataFiles(options) {

  val tables: Map[String, BaseDataFileTable] = dasOptions.tableConfigs.map { config =>
    if (!config.url.startsWith("http:") && !config.url.startsWith("https:")) {
      throw new DASSdkInvalidArgumentException(s"Unsupported URL ${config.url}")
    }
    config.name -> new XmlTable(config, sparkSession, hppFileCache)
  }.toMap

}

/**
 * Builder for the "http-csv" DAS type. The engine calls build() with user-provided config, returning a new DasS3Csv
 * instance.
 */
class DASXmlHttpBuilder extends DASSdkBuilder {

  // This must match your "type" field in the config for the plugin
  override def dasType: String = "s3-csv"

  override def build(options: Map[String, String])(implicit settings: DASSettings): DASSdk = {
    new DASXmlHttp(options)
  }
}
