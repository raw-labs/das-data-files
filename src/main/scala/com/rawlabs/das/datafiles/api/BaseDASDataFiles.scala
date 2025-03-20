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

package com.rawlabs.das.datafiles.api

import java.net.URI

import scala.collection.mutable

import org.apache.spark.sql.SparkSession

import com.rawlabs.das.datafiles.filesystem.{FileCacheManager, FileSystemError, FileSystemFactory}
import com.rawlabs.das.datafiles.utils.{DASDataFilesOptions, SparkSessionBuilder}
import com.rawlabs.das.sdk.scala.{DASFunction, DASSdk, DASTable}
import com.rawlabs.das.sdk.{
  DASSdkInvalidArgumentException,
  DASSdkPermissionDeniedException,
  DASSdkUnauthenticatedException
}
import com.rawlabs.protocol.das.v1.functions.FunctionDefinition
import com.rawlabs.protocol.das.v1.tables.TableDefinition
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

case class DataFilesTableConfig(
    uri: URI,
    name: String,
    format: Option[String],
    options: Map[String, String],
    fileCacheManager: FileCacheManager)

/**
 * The main plugin class that registers one table per file.
 */
abstract class BaseDASDataFiles(options: Map[String, String]) extends DASSdk with StrictLogging {

  import BaseDASDataFiles._

  private val dasOptions = new DASDataFilesOptions(options)

  // Keep track of used names so we ensure uniqueness
  private val usedNames = mutable.Set[String]()

  protected lazy val sparkSession: SparkSession = SparkSessionBuilder.build("dasDataFilesApp", options)

  private val filesystems = {
    // Build a map of filesystems by scheme we only need one of each type
    val oneOfEachScheme = dasOptions.pathConfig.map(x => x.uri.getScheme -> x.uri).toMap

    oneOfEachScheme.map { case (scheme, uri) =>
      scheme -> FileSystemFactory.build(uri, options)
    }
  }

  private val fileCacheManager: FileCacheManager =
    new FileCacheManager(filesystems.values.toSeq, fileCacheExpiration, cleanupCachePeriod)

  // Resolve all URLs and build a list of tables
  protected val tableConfig: Seq[DataFilesTableConfig] = dasOptions.pathConfig.flatMap { config =>
    val filesystem = filesystems(config.uri.getScheme)

    val urls = filesystem.resolveWildcard(config.uri.toString) match {
      case Right(url) => url
      case Left(FileSystemError.NotFound(_)) =>
        throw new DASSdkInvalidArgumentException(s"No files found at ${config.uri}")
      case Left(FileSystemError.PermissionDenied(msg)) => throw new DASSdkPermissionDeniedException(msg)
      case Left(FileSystemError.Unauthorized(msg))     => throw new DASSdkUnauthenticatedException(msg)
      case Left(FileSystemError.Unsupported(msg))      => throw new DASSdkInvalidArgumentException(msg)
      case Left(FileSystemError.TooManyRequests(msg))  => throw new DASSdkInvalidArgumentException(msg)
      case _                                           => throw new DASSdkInvalidArgumentException("Unexpected error")
    }

    if (urls.length > 1) logger.debug("Multiple URLs found: {}", urls.mkString(", "))

    urls.map { url =>
      val name = if (urls.length == 1 && config.maybeName.isDefined) {
        // name is provided and there is only one URL
        config.maybeName.get
      } else if (urls.length > 1 && config.maybeName.isDefined) {
        // name is provided and there are multiple URLs
        val prefix = config.maybeName.get
        val suffix = deriveNameFromUrl(url)
        s"${prefix}_$suffix"
      } else {
        // name is not provided
        deriveNameFromUrl(url)
      }

      val unique = ensureUniqueName(name)
      DataFilesTableConfig(new URI(url), unique, config.maybeFormat, config.options, fileCacheManager)
    }
  }

  if (tableConfig.length > maxTables) {
    throw new IllegalArgumentException(s"Too many tables: ${tableConfig.length} > $maxTables")
  }

  logger.info("Adding tables: {}", tableConfig.map(_.name).mkString(", "))

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
    filesystems.values.foreach(_.stop())
    fileCacheManager.stop()
  }

  /**
   * Given a URL, derive the table name from the filename. E.g. "https://host/path/data.csv" => "data_csv"
   */
  private def deriveNameFromUrl(url: String): String = {
    // Extract last path segment
    val filePart = url.split("/").lastOption.getOrElse(url)
    val withoutExtension = filePart.lastIndexOf(".") match {
      case -1  => filePart
      case idx => filePart.substring(0, idx)
    }
    // Replace other dots with '_'
    withoutExtension.replace('.', '_')
  }

  /**
   * Ensure the proposed name is unique by appending _2, _3, etc. as needed.
   */
  private def ensureUniqueName(base: String): String = {
    var finalName = base
    var n = 2
    while (usedNames.contains(finalName)) {
      finalName = s"${base}_$n"
      n += 1
    }
    usedNames += finalName
    finalName
  }

}

object BaseDASDataFiles {

  private val config = ConfigFactory.load()
  private val maxTables = config.getInt("raw.das.data-files.max-tables")
  private val fileCacheExpiration = config.getInt("raw.das.data-files.file-cache-expiration")
  private val cleanupCachePeriod = config.getInt("raw.das.data-files.cleanup-cache-period")

}
