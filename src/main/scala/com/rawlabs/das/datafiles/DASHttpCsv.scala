package com.rawlabs.das.datafiles

import com.rawlabs.das.datafiles.table.{BaseDataFileTable, CsvTable}
import com.rawlabs.das.sdk.scala.{DASSdk, DASSdkBuilder}
import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSettings}

class DASHttpCsv(options: Map[String, String]) extends BaseDASDataFiles(options) {

  val tables: Map[String, BaseDataFileTable] = dasOptions.tableConfigs.map { config =>
    if (!config.url.startsWith("http:") || !config.url.startsWith("https:")) {
      throw new DASSdkInvalidArgumentException(s"Unsupported URL ${config.url}")
    }
    config.name -> new CsvTable(config, sparkSession, hppFileCache)
  }.toMap

}

/**
 * Builder for the "http-csv" DAS type. The engine calls build() with the user-provided config, returning a new DASHttp
 * instance.
 */
class DASHttpCsvBuilder extends DASSdkBuilder {

  // This must match your "type" field in the config for the plugin
  override def dasType: String = "http-csv"

  override def build(options: Map[String, String])(implicit settings: DASSettings): DASSdk = {
    new DASHttpCsv(options)
  }
}
