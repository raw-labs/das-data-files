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

import com.rawlabs.das.datafiles.api.{BaseDASDataFiles, BaseDataFileTable}
import com.rawlabs.das.sdk.DASSettings
import com.rawlabs.das.sdk.scala.{DASSdk, DASSdkBuilder}

/**
 * The main plugin class that registers one table per file.
 */
class DASXml(options: Map[String, String])(implicit settings: DASSettings) extends BaseDASDataFiles(options) {

  // Build a list of our tables
  val tables: Map[String, BaseDataFileTable] = tableConfig.map { config =>
    config.name -> new XmlTable(config, sparkSession)
  }.toMap

}

class DASXmlBuilder extends DASSdkBuilder {

  // This must match your "type" field in the config for the plugin
  override def dasType: String = "xml"

  override def build(options: Map[String, String])(implicit settings: DASSettings): DASSdk = {
    new DASXml(options)
  }
}
