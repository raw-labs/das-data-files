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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.rawlabs.das.datafiles.filesystem.FileSystemError
import com.rawlabs.das.datafiles.utils.SparkToDASConverter
import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.das.sdk.{
  DASExecuteResult,
  DASSdkInvalidArgumentException,
  DASSdkPermissionDeniedException,
  DASSdkUnauthenticatedException
}
import com.rawlabs.protocol.das.v1.query.{Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.{
  Column => ProtoColumn,
  ColumnDefinition,
  Row => ProtoRow,
  TableDefinition,
  TableId
}
import com.rawlabs.protocol.das.v1.types._
import com.typesafe.scalalogging.StrictLogging

/**
 * An abstract base class for "Data File" tables. Common logic:
 *   - Storing tableName, url, SparkSession
 *   - Overriding basic DASTable methods
 *   - Inferring schema / building TableDefinition (if desired)
 *
 * Child classes implement: def loadDataFrame(): DataFrame
 */
abstract class BaseDataFileTable(config: DataFilesTableConfig, sparkSession: SparkSession)
    extends DASTable
    with SparkToDASConverter
    with StrictLogging {

  private val tableName: String = config.name

  val format: String

  protected val sparkOptions: Map[String, String]

  /**
   * Convert the Spark schema to a list of (colName -> DAS Type)
   */
  private lazy val columns: Seq[(String, Type)] = {
    val executionUrl = acquireUrl()
    try {
      val dfSchema = loadDataFrame(executionUrl).schema
      dfSchema.fields.toIndexedSeq.map { field =>
        val dasType = sparkTypeToDAS(field.dataType, field.nullable)
        (field.name, dasType)
      }
    } finally {
      // deletes the file if needed (github)
      releaseFile(executionUrl)
    }
  }

  // Build the TableDefinition from the columns that we got from the dataframe schema
  lazy val tableDefinition: TableDefinition = {
    val builder = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName(tableName))
      .setDescription(s"Table for $format data from ${config.uri}")

    columns.foreach { case (colName, colType) =>
      builder.addColumns(ColumnDefinition.newBuilder().setName(colName).setType(colType))
    }

    builder.build()
  }

  override def getTablePathKeys: Seq[com.rawlabs.protocol.das.v1.query.PathKey] = Seq.empty

  override def getTableSortOrders(sortKeys: Seq[SortKey]): Seq[SortKey] = sortKeys.filter(x => x.getCollate.isEmpty)

  /**
   * The main data read flow: 1) loadDataFrame() [abstract method implemented by child classes] 2) applyQuals (pushdown
   * filtering) 3) select requested columns 4) applySortKeys 5) limit 6) convert to DAS rows
   */
  override def execute(
      quals: Seq[Qual],
      columnsRequested: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long]): DASExecuteResult = {

    val executionUrl = acquireUrl()

    logger.debug(s"Executing $format table $tableName format  on $executionUrl, original url: ${config.uri}")
    val df = loadDataFrame(executionUrl)
    val filteredDF = applyQuals(df, quals)

    val finalCols = if (columnsRequested.nonEmpty) columnsRequested else filteredDF.columns.toSeq
    val dfSelected = filteredDF.select(finalCols.map(df.col): _*)

    // applySortKeys *before* limit
    val dfSorted = applySortKeys(dfSelected, sortKeys)

    val dfLimited = maybeLimit match {
      case Some(l) => dfSorted.limit(l.toInt)
      case None    => dfSorted
    }

    val sparkIter = dfLimited.toLocalIterator().asScala

    // For quick lookup of col -> DAS Type
    val colTypesMap: Map[String, Type] = columns.toMap

    new DASExecuteResult {
      private var cleanedUp = false

      // Delete the file if needed when the iterator is exhausted
      override def hasNext: Boolean = {
        val hn = sparkIter.hasNext
        if (!hn && !cleanedUp) {
          releaseFile(executionUrl)
          cleanedUp = true
        }
        hn
      }

      override def next(): ProtoRow = {
        val rowBuilder = ProtoRow.newBuilder()
        val row = sparkIter.next()

        finalCols.foreach { col =>
          val rawVal = row.getAs[Any](col)
          val dasType =
            colTypesMap.getOrElse(
              col,
              throw new DASSdkInvalidArgumentException(s"table $tableName Column $col not found in schema"))
          val protoVal = sparkValueToProtoValue(rawVal, dasType, col)
          rowBuilder.addColumns(ProtoColumn.newBuilder().setName(col).setData(protoVal))
        }
        rowBuilder.build()
      }
      // Delete the file if needed when the iterator is exhausted
      override def close(): Unit = {
        if (!cleanedUp) {
          releaseFile(executionUrl)
          cleanedUp = true
        }
      }
    }

  }

  // Mark read-only
  override def insert(row: ProtoRow): ProtoRow =
    throw new DASSdkInvalidArgumentException(s"DataFile table '$tableName' is read-only.")

  override def update(rowId: Value, newRow: ProtoRow): ProtoRow =
    throw new DASSdkInvalidArgumentException(s"DataFile table '$tableName' is read-only.")

  override def delete(rowId: Value): Unit =
    throw new DASSdkInvalidArgumentException(s"DataFile table '$tableName' is read-only.")

  protected def loadDataFrame(resolvedUrl: String): DataFrame = {
    sparkSession.read
      .options(sparkOptions)
      .format(format)
      .load(resolvedUrl)
  }

  private def acquireUrl(): String = {
    // sparks support s3 filesystem directly so convert it to s3a
    if (config.uri.getScheme == "s3") {
      "s3a://" + config.uri.getAuthority + config.uri.getPath
    } else if (config.uri.getScheme == "s3a") {
      config.uri.toString
    } else {
      config.fileCacheManager.getLocalPathForUrl(config.uri.toString) match {
        case Right(url) => url
        case Left(FileSystemError.NotFound(_)) =>
          throw new DASSdkInvalidArgumentException(s"No files found at ${config.uri}")
        case Left(FileSystemError.PermissionDenied(msg)) => throw new DASSdkPermissionDeniedException(msg)
        case Left(FileSystemError.Unauthorized(msg))     => throw new DASSdkUnauthenticatedException(msg)
        case Left(FileSystemError.Unsupported(msg))      => throw new DASSdkInvalidArgumentException(msg)
        case Left(FileSystemError.TooManyRequests(msg))  => throw new DASSdkInvalidArgumentException(msg)
        case Left(FileSystemError.InvalidUrl(url, message)) =>
          throw new DASSdkInvalidArgumentException(s"Invalid URL:$url, $message")
        case Left(FileSystemError.FileTooLarge(url, actualSize, maxLocalFileSize)) =>
          throw new DASSdkInvalidArgumentException(s"File too large: $url ($actualSize > $maxLocalFileSize)")
      }
    }
  }

  private def releaseFile(resolved: String): Unit = {
    logger.debug("Releasing file: {}", resolved)
  }

  // Helper to remap options from our custom keys to Spark keys
  protected def remapOptions(options: Map[String, String]): Map[String, String] = {
    options.flatMap { case (key, option) =>
      config.options.get(key).map(value => option -> value)
    }
  }

}
