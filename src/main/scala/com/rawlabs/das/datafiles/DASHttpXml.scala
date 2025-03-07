package com.rawlabs.das.datafiles

import com.rawlabs.das.datafiles.table.{BaseDataFileTable, CsvTable, XmlTable}
import com.rawlabs.das.sdk.scala.{DASSdk, DASSdkBuilder}
import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSettings}

class DASHttpXml(options: Map[String, String]) extends BaseDASDataFiles(options) {

  val tables: Map[String, BaseDataFileTable] = dasOptions.tableConfigs.map { config =>
    if (!config.url.startsWith("http:") || !config.url.startsWith("https:")) {
      throw new DASSdkInvalidArgumentException(s"Unsupported URL ${config.url}")
    }
    config.name -> new XmlTable(config, sparkSession, hppFileCache)
  }.toMap

}

/**
 * Builder for the "http-csv" DAS type. The engine calls build() with user-provided config, returning a new DasS3Csv
 * instance.
 */
class DASHttpXmlBuilder extends DASSdkBuilder {

  // This must match your "type" field in the config for the plugin
  override def dasType: String = "s3-csv"

  override def build(options: Map[String, String])(implicit settings: DASSettings): DASSdk = {
    new DASHttpXml(options)
  }
}
