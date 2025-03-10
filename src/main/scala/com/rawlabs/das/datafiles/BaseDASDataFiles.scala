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

import com.rawlabs.das.sdk.scala.{DASFunction, DASSdk, DASTable}
import com.rawlabs.protocol.das.v1.functions.FunctionDefinition
import com.rawlabs.protocol.das.v1.tables.TableDefinition

/**
 * The main plugin class that registers one table per file.
 */
abstract class BaseDASDataFiles(options: Map[String, String]) extends DASSdk {
  protected val dasOptions = new DASDataFilesOptions(options)

  protected lazy val sparkSession: SparkSession = SParkSessionBuilder.build("dasDataFilesApp", dasOptions)

  protected val hppFileCache: HttpFileCache = HttpFileCache.build(dasOptions.httpOptions)

  // Build a list of our tables
  def tables: Map[String, BaseDataFileTable]

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
