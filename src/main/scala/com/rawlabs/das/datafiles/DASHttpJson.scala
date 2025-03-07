package com.rawlabs.das.datafiles

import com.rawlabs.das.datafiles.table.{BaseDataFileTable, JsonTable}
import com.rawlabs.das.sdk.scala.{DASSdk, DASSdkBuilder}
import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSettings}

class DASHttpJson(options: Map[String, String]) extends BaseDASDataFiles(options) {

  val tables: Map[String, BaseDataFileTable] = dasOptions.tableConfigs.map { config =>
    if (!config.url.startsWith("http:") || !config.url.startsWith("https:")) {
      throw new DASSdkInvalidArgumentException(s"Unsupported URL ${config.url}")
    }
    config.name -> new JsonTable(config, sparkSession, hppFileCache)
  }.toMap

}

class DASHttpJsonBuilder extends DASSdkBuilder {

  // This must match your "type" field in the config for the plugin
  override def dasType: String = "http-json"

  override def build(options: Map[String, String])(implicit settings: DASSettings): DASSdk = {
    new DASHttpJson(options)
  }
}
