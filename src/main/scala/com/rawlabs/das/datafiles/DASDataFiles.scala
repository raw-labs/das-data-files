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

package com.rawlabs.das.datafiles

import org.apache.spark.sql.SparkSession

import com.rawlabs.das.datafiles.DASDataFiles.buildSparkSession
import com.rawlabs.das.sdk.DASSdkException
import com.rawlabs.das.sdk.scala.{DASFunction, DASSdk, DASTable}
import com.rawlabs.protocol.das.v1.functions.FunctionDefinition
import com.rawlabs.protocol.das.v1.tables.TableDefinition

object DASDataFiles {

  def buildSparkSession(appName: String, options: DASDataFilesOptions): SparkSession = {
    val builder = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]") // or read from some config

    options.s3Credentials.foreach { creds =>
      builder.config("fs.s3a.access.key", creds.accessKey)
      builder.config("fs.s3a.secret.key", creds.secretKey)
      // disabling s3 metrics
      builder.config("fs.s3a.metrics.conf", "")
    }
    builder.config(options.extraSparkConfig)
    builder.getOrCreate()
  }
}

/**
 * The main plugin class that registers one table per file.
 */
class DASDataFiles(options: Map[String, String]) extends DASSdk {
  private val dasOptions = new DASDataFilesOptions(options)

  private lazy val sparkSession = buildSparkSession("dasDataFilesApp", dasOptions)

  // Build a list of our tables
  private val tables: Map[String, BaseDataFileTable] = dasOptions.tableConfigs.map { config =>
    val format = config.format.getOrElse(throw new DASSdkException(s"format not specified for table ${config.name}"))
    format match {
      case "csv"     => config.name -> new CsvTable(config, sparkSession, HttpFileCache.global)
      case "json"    => config.name -> new JsonTable(config, sparkSession, HttpFileCache.global)
      case "parquet" => config.name -> new ParquetTable(config, sparkSession, HttpFileCache.global)
      case "xml"     => config.name -> new XmlTable(config, sparkSession, HttpFileCache.global)
      case other     => throw new DASSdkException(s"Unsupported format $other")
    }
  }.toMap

  // Return the definitions to the engine
  override def tableDefinitions: Seq[TableDefinition] = tables.values.map(_.tableDefinition).toSeq

  // This plugin has no custom functions
  override def functionDefinitions: Seq[FunctionDefinition] = Seq.empty

  /**
   * Return the table instance for the requested name (if found).
   */
  override def getTable(name: String): Option[DASTable] = tables.get(name)

  /**
   * No custom functions
   */
  override def getFunction(name: String): Option[DASFunction] = None

}
