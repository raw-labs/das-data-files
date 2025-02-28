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

package com.rawlabs.das.datafiles

import com.rawlabs.das.sdk.scala.{DASFunction, DASSdk, DASTable}
import com.rawlabs.protocol.das.v1.functions.FunctionDefinition
import com.rawlabs.protocol.das.v1.tables.TableDefinition

/**
 * The main plugin class that registers one table per file.
 */
class DASDataFiles(options: Map[String, String]) extends DASSdk {

  // For example, create a Spark session that all tables share.
  // In a production plugin, you might configure Spark more carefully:
  private lazy val spark = {
    import org.apache.spark.sql.SparkSession
    SparkSession
      .builder()
      .appName("DASDataFiles")
      .master("local[*]") // or read from some config
      .getOrCreate()
  }

  private val fileOptions = new DASDataFilesOptions(options)

  // Build a list of our tables
  private val tables: Map[String, BaseDataFileTable] = fileOptions.tableConfigs.map { config =>
    config.format match {
      case "csv" => config.name -> new CsvTable(config.name, config.url, config.options, spark)
      case "json" => config.name -> new JsonTable(config.name, config.url, config.options, spark)
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
