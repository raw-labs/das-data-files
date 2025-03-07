package com.rawlabs.das.datafiles

import com.rawlabs.das.datafiles.table.{BaseDataFileTable, CsvTable, ParquetTable}
import com.rawlabs.das.sdk.scala.{DASSdk, DASSdkBuilder}
import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSettings}

class DASHttpParquet(options: Map[String, String]) extends BaseDASDataFiles(options) {

  val tables: Map[String, BaseDataFileTable] = dasOptions.tableConfigs.map { config =>
    if (!config.url.startsWith("http:") || !config.url.startsWith("https:")) {
      throw new DASSdkInvalidArgumentException(s"Unsupported URL ${config.url}")
    }
    config.name -> new ParquetTable(config, sparkSession, hppFileCache)
  }.toMap

}

class DASHttpParquetBuilder extends DASSdkBuilder {

  // This must match your "type" field in the config for the plugin
  override def dasType: String = "http-parquet"

  override def build(options: Map[String, String])(implicit settings: DASSettings): DASSdk = {
    new DASHttpParquet(options)
  }
}
