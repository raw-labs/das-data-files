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

import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.apache.spark.sql.SparkSession

import com.rawlabs.das.sdk.DASSdkInvalidArgumentException
import com.rawlabs.das.sdk.scala.{DASFunction, DASSdk, DASTable}
import com.rawlabs.protocol.das.v1.functions.FunctionDefinition
import com.rawlabs.protocol.das.v1.tables.TableDefinition
import com.typesafe.config.{Config, ConfigFactory}

object DASDataFiles {
  private val config: Config = ConfigFactory.load()
  private val cacheDirStr = config.getString("raw.das.data-files.cache-dir")
  private val idleTimeoutMillis = config.getLong("raw.das.data-files.cache-idle-timeout-ms")
  private val evictionCheckMillis = config.getLong("raw.das.data-files.cache-eviction-check-ms")

}

/**
 * The main plugin class that registers one table per file.
 */
class DASDataFiles(options: Map[String, String]) extends DASSdk {
  import DASDataFiles._
  private val dasOptions = new DASDataFilesOptions(options)

  private lazy val sparkSession = SParkSessionBuilder.build("dasDataFilesApp", dasOptions)

  private val hppFileCache =
    new HttpFileCache(cacheDirStr, idleTimeoutMillis, evictionCheckMillis, dasOptions.httpOptions)
  // Build a list of our tables
  private val tables: Map[String, BaseDataFileTable] = dasOptions.tableConfigs.map { config =>
    val format = config.format.getOrElse(
      throw new DASSdkInvalidArgumentException(s"format not specified for table ${config.name}"))
    format match {
      case "csv"     => config.name -> new CsvTable(config, sparkSession, hppFileCache)
      case "json"    => config.name -> new JsonTable(config, sparkSession, hppFileCache)
      case "parquet" => config.name -> new ParquetTable(config, sparkSession, hppFileCache)
      case "xml"     => config.name -> new XmlTable(config, sparkSession, hppFileCache)
      case other     => throw new DASSdkInvalidArgumentException(s"Unsupported format $other")
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

  override def close(): Unit = {
    sparkSession.stop()
    hppFileCache.shutdown()
  }

}
