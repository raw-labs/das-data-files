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

import java.io.File

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column => SparkColumn, DataFrame, Row, SparkSession, types => sparkTypes}

import com.rawlabs.das.datafiles.filesystem.FileSystemError
import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.das.sdk.{
  DASExecuteResult,
  DASSdkInvalidArgumentException,
  DASSdkPermissionDeniedException,
  DASSdkUnauthenticatedException
}
import com.rawlabs.protocol.das.v1.query.{Operator, Qual, SortKey}
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
    with StrictLogging {

  val tableName: String = config.name

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
            colTypesMap.getOrElse(col, throw new RuntimeException(s"table $tableName Column $col not found in schema"))
          val protoVal = rawToProtoValue(rawVal, dasType, col)
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

  // -------------------------------------------------------------------
  // Optionally, common pushdown logic
  // -------------------------------------------------------------------
  private def applyQuals(df: DataFrame, quals: Seq[Qual]): DataFrame = {

    var result = df

    for (q <- quals) {
      if (q.hasSimpleQual) {
        val sq = q.getSimpleQual
        val op = sq.getOperator
        val colName = q.getName
        val valProto = sq.getValue

        val filterCol = col(colName)

        // Convert the proto Value into a native Scala type
        val typedValue: Any = {
          if (valProto.hasNull) {
            null
          } else if (valProto.hasString) {
            valProto.getString.getV
          } else if (valProto.hasInt) {
            valProto.getInt.getV
          } else if (valProto.hasLong) {
            valProto.getLong.getV
          } else if (valProto.hasBool) {
            valProto.getBool.getV
          } else if (valProto.hasDouble) {
            valProto.getDouble.getV
          } else if (valProto.hasFloat) {
            valProto.getFloat.getV
          } else {
            throw new DASSdkInvalidArgumentException(s"Unsupported filter type on col=$colName in base class.")
          }
        }

        // Build the Spark filter expression based on the operator
        val condition = op match {
          case Operator.EQUALS if valProto.hasNull     => filterCol.isNull
          case Operator.NOT_EQUALS if valProto.hasNull => filterCol.isNotNull
          case Operator.EQUALS                         => filterCol === typedValue
          case Operator.NOT_EQUALS                     => filterCol =!= typedValue
          case Operator.LESS_THAN                      => filterCol < typedValue
          case Operator.LESS_THAN_OR_EQUAL             => filterCol <= typedValue
          case Operator.GREATER_THAN                   => filterCol > typedValue
          case Operator.GREATER_THAN_OR_EQUAL          => filterCol >= typedValue
          case Operator.LIKE =>
            if (!valProto.hasString) {
              throw new DASSdkInvalidArgumentException("LIKE operator requires a string value")
            }
            filterCol.like(typedValue.toString)
          case Operator.NOT_LIKE =>
            if (!valProto.hasString) {
              throw new DASSdkInvalidArgumentException("NOT LIKE operator requires a string value")
            }
            not(filterCol.like(typedValue.toString))
          case Operator.ILIKE =>
            if (!valProto.hasString) {
              throw new DASSdkInvalidArgumentException("ILIKE operator requires a string value")
            }
            lower(filterCol).like(typedValue.toString.toLowerCase)
          case Operator.NOT_ILIKE =>
            if (!valProto.hasString) {
              throw new DASSdkInvalidArgumentException("NOT ILIKE operator requires a string value")
            }
            not(lower(filterCol).like(typedValue.toString.toLowerCase))
          case other =>
            throw new DASSdkInvalidArgumentException(s"Operator $other not supported in base class.")
        }

        result = result.filter(condition)
      }
    }

    result
  }

  private def acquireUrl(): String = {
    // sparks support s3 filesystem directly so convert it to s3a
    if (config.uri.getScheme == "s3") {
      "s3a://" + config.uri.getAuthority + config.uri.getPath
    } else if (config.uri.getScheme == "s3a") {
      config.uri.toString
    } else {
      config.filesystem.getLocalUrl(config.uri.toString) match {
        case Right(url) => url
        case Left(FileSystemError.NotFound(_)) =>
          throw new DASSdkInvalidArgumentException(s"No files found at ${config.uri}")
        case Left(FileSystemError.PermissionDenied(msg)) => throw new DASSdkPermissionDeniedException(msg)
        case Left(FileSystemError.Unauthorized(msg))     => throw new DASSdkUnauthenticatedException(msg)
        case Left(FileSystemError.Unsupported(msg))      => throw new DASSdkInvalidArgumentException(msg)
        case Left(FileSystemError.TooManyRequests(msg))  => throw new DASSdkInvalidArgumentException(msg)
        case Left(FileSystemError.GenericError(msg, e))  => throw new DASSdkInvalidArgumentException(msg, e)
        case Left(FileSystemError.FileTooLarge(url, actualSize, maxLocalFileSize)) =>
          throw new DASSdkInvalidArgumentException(s"File too large: $url ($actualSize > $maxLocalFileSize)")
      }
    }
  }

  private def releaseFile(resolved: String): Unit = {
    if (config.uri.getScheme == "github") {
      val file = new File(resolved)
      file.delete()
    }
  }

  // -------------------------------------------------------------------
  // apply sort keys
  // -------------------------------------------------------------------
  /**
   * For each SortKey, build a Spark Column with ascending or descending order, plus optional nulls first/last if you
   * want to handle that.
   *
   * Note: In your proto, you have `bool is_reversed` and `bool nulls_first`.
   */
  private def applySortKeys(df: DataFrame, sortKeys: Seq[SortKey]): DataFrame = {
    if (sortKeys.isEmpty) {
      df
    } else {
      val sortCols: Seq[SparkColumn] = sortKeys.map { sk =>
        // if reversed => desc
        if (sk.getIsReversed && sk.getNullsFirst) {
          df.col(sk.getName).desc_nulls_first
        } else if (sk.getIsReversed && !sk.getNullsFirst) {
          df.col(sk.getName).desc_nulls_last
        } else if (!sk.getIsReversed && sk.getNullsFirst) {
          df.col(sk.getName).asc_nulls_first
        } else {
          df.col(sk.getName).asc_nulls_last
        }

      }
      df.orderBy(sortCols: _*)
    }
  }

  // Helper to remap options from our custom keys to Spark keys
  protected def remapOptions(options: Map[String, String]): Map[String, String] = {
    options.flatMap { case (key, option) =>
      config.options.get(key).map(value => option -> value)
    }
  }

  // -------------------------------------------------------------------
  //  Utilities for schema mapping, building proto Values, etc.
  // -------------------------------------------------------------------
  /**
   * Convert a Spark DataType to a DAS Type, handling:
   *   - basic primitives (Byte, Short, Int, Long, Float, Double, Bool, String)
   *   - DecimalType => Decimal
   *   - DateType => Date
   *   - TimestampType => Timestamp
   *   - ArrayType => ListType
   *   - StructType => RecordType
   *   - MapType => (optionally) RecordType
   * Anything unrecognized falls back to a string type by default.
   */
  private def sparkTypeToDAS(
      sparkType: org.apache.spark.sql.types.DataType,
      nullable: Boolean): com.rawlabs.protocol.das.v1.types.Type = {

    import com.rawlabs.protocol.das.v1.types.{Type => DasType, _}

    sparkType match {

      // -----------------------------------------
      // 1) Basic numeric types
      // -----------------------------------------
      case sparkTypes.ByteType =>
        DasType
          .newBuilder()
          .setByte(ByteType.newBuilder().setNullable(nullable))
          .build()

      case sparkTypes.ShortType =>
        DasType
          .newBuilder()
          .setShort(ShortType.newBuilder().setNullable(nullable))
          .build()

      case sparkTypes.IntegerType =>
        DasType
          .newBuilder()
          .setInt(IntType.newBuilder().setNullable(nullable))
          .build()

      case sparkTypes.LongType =>
        DasType
          .newBuilder()
          .setLong(LongType.newBuilder().setNullable(nullable))
          .build()

      case sparkTypes.FloatType =>
        DasType
          .newBuilder()
          .setFloat(FloatType.newBuilder().setNullable(nullable))
          .build()

      case sparkTypes.DoubleType =>
        DasType
          .newBuilder()
          .setDouble(DoubleType.newBuilder().setNullable(nullable))
          .build()

      // -----------------------------------------
      // 2) Boolean
      // -----------------------------------------
      case sparkTypes.BooleanType =>
        DasType
          .newBuilder()
          .setBool(BoolType.newBuilder().setNullable(nullable))
          .build()

      // -----------------------------------------
      // 3) Decimal
      // -----------------------------------------
      case _: sparkTypes.DecimalType =>
        // We lose precision/scale metadata in the "DecimalType" proto,
        // but at least we flag it's decimal.
        DasType
          .newBuilder()
          .setDecimal(DecimalType.newBuilder().setNullable(nullable))
          .build()

      // -----------------------------------------
      // 4) String
      // -----------------------------------------
      case sparkTypes.StringType =>
        DasType
          .newBuilder()
          .setString(StringType.newBuilder().setNullable(nullable))
          .build()

      // -----------------------------------------
      // 5) Date & Timestamp
      // -----------------------------------------
      case sparkTypes.DateType =>
        DasType
          .newBuilder()
          .setDate(DateType.newBuilder().setNullable(nullable))
          .build()

      case sparkTypes.TimestampType =>
        DasType
          .newBuilder()
          .setTimestamp(TimestampType.newBuilder().setNullable(nullable))
          .build()

      // -----------------------------------------
      // 6) Binary
      // -----------------------------------------
      case sparkTypes.BinaryType =>
        DasType
          .newBuilder()
          .setBinary(BinaryType.newBuilder().setNullable(nullable))
          .build()

      // -----------------------------------------
      // 7) Arrays => ListType
      // -----------------------------------------
      case sparkTypes.ArrayType(elementType, elementContainsNull) =>
        val elementDasType = sparkTypeToDAS(elementType, elementContainsNull)
        DasType
          .newBuilder()
          .setList(
            ListType
              .newBuilder()
              .setInnerType(elementDasType)
              .setNullable(nullable))
          .build()

      // -----------------------------------------
      // 8) Struct => RecordType
      // -----------------------------------------
      case sparkTypes.StructType(fields) =>
        // Convert each Spark field => an AttrType
        val attsBuilder = RecordType.newBuilder().setNullable(nullable)
        fields.foreach { field =>
          val dasFieldType = sparkTypeToDAS(field.dataType, field.nullable)
          attsBuilder.addAtts(
            AttrType
              .newBuilder()
              .setName(field.name)
              .setTipe(dasFieldType))
        }

        DasType
          .newBuilder()
          .setRecord(attsBuilder.build())
          .build()

      // -----------------------------------------
      // 9) Map => (optionally) RecordType or fallback
      // -----------------------------------------
      case sparkTypes.MapType(keyType, valueType, valueContainsNull) =>
        // The DAS proto does not have a native map type,
        // so you can store it as a record with "key" and "value" list, or something else.
        //
        // For example, treat map as a struct with two arrays: "keys" and "values":
        // (This is one approach. Another approach is to treat each entry as a record.)
        val mapAsRecord = RecordType.newBuilder().setNullable(nullable)

        // Create an AttrType for "keys"
        val keysType = sparkTypeToDAS(keyType, nullable = false)
        val keysListType = DasType
          .newBuilder()
          .setList(
            ListType
              .newBuilder()
              .setInnerType(keysType)
              .setNullable(true))
          .build()

        mapAsRecord.addAtts(
          AttrType
            .newBuilder()
            .setName("keys")
            .setTipe(keysListType))

        // Create an AttrType for "values"
        val valsType = sparkTypeToDAS(valueType, valueContainsNull)
        val valsListType = DasType
          .newBuilder()
          .setList(
            ListType
              .newBuilder()
              .setInnerType(valsType)
              .setNullable(true))
          .build()

        mapAsRecord.addAtts(
          AttrType
            .newBuilder()
            .setName("values")
            .setTipe(valsListType))

        DasType
          .newBuilder()
          .setRecord(mapAsRecord.build())
          .build()

      // -----------------------------------------
      // Unknown type
      // -----------------------------------------
      case other =>
        throw new DASSdkInvalidArgumentException(s"Unsupported Spark type: ${other.typeName}")
    }
  }

  /**
   * Recursively converts a raw Spark value into a DAS Value, guided by a DAS Type (i.e. from 'types.proto').
   *
   * Handles:
   *   - primitives (int, long, bool, float, double, string)
   *   - arrays => ValueList
   *   - struct => ValueRecord
   *   - optional map => stored as a ValueRecord (or fallback)
   *   - decimal, binary, etc. if you need them
   */
  private def rawToProtoValue(rawValue: Any, dasType: Type, colName: String): Value = {

    // 0) Null check
    if (rawValue == null) {
      return Value
        .newBuilder()
        .setNull(ValueNull.newBuilder().build())
        .build()
    }
    // 1) If the type is known primitive
    else if (dasType.hasString) {
      Value
        .newBuilder()
        .setString(ValueString.newBuilder().setV(rawValue.toString))
        .build()
    } else if (dasType.hasInt) {
      val intVal = rawValue match {
        case i: Int    => i
        case l: Long   => l.toInt
        case s: String => s.toInt
        case _ =>
          throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to int ($colName)")
      }
      Value
        .newBuilder()
        .setInt(ValueInt.newBuilder().setV(intVal))
        .build()
    } else if (dasType.hasLong) {
      val longVal = rawValue match {
        case l: Long   => l
        case i: Int    => i.toLong
        case s: String => s.toLong
        case _ =>
          throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to long ($colName)")
      }
      Value
        .newBuilder()
        .setLong(ValueLong.newBuilder().setV(longVal))
        .build()
    } else if (dasType.hasBool) {
      val boolVal = rawValue match {
        case b: Boolean => b
        case s: String  => s.toBoolean
        case _ =>
          throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to bool ($colName)")
      }
      Value
        .newBuilder()
        .setBool(ValueBool.newBuilder().setV(boolVal))
        .build()
    } else if (dasType.hasDouble) {
      val dblVal = rawValue match {
        case d: Double => d
        case f: Float  => f.toDouble
        case s: String => s.toDouble
        case _ =>
          throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to double ($colName)")
      }
      Value
        .newBuilder()
        .setDouble(ValueDouble.newBuilder().setV(dblVal))
        .build()
    } else if (dasType.hasFloat) {
      val fltVal = rawValue match {
        case f: Float  => f
        case d: Double => d.toFloat
        case s: String => s.toFloat
        case _ =>
          throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to float ($colName)")
      }
      Value
        .newBuilder()
        .setFloat(ValueFloat.newBuilder().setV(fltVal))
        .build()
    }

    // 2) If we have a list/array type => ValueList
    else if (dasType.hasList) {
      val innerType = dasType.getList.getInnerType
      val seq = rawValue match {
        case s: Seq[_]         => s
        case arr: Array[_]     => arr.toSeq
        case iter: Iterable[_] => iter.toSeq
        // Spark often uses WrappedArray for arrays
        case other =>
          throw new DASSdkInvalidArgumentException(s"Cannot convert $other to list type for col=$colName")
      }

      val listBuilder = ValueList.newBuilder()
      seq.foreach { elem =>
        val itemValue = rawToProtoValue(elem, innerType, colName + "_elem")
        listBuilder.addValues(itemValue)
      }
      Value.newBuilder().setList(listBuilder.build()).build()
    }

    // 3) If we have a record/struct => ValueRecord
    else if (dasType.hasRecord) {
      // Typically Spark uses Row for struct fields
      rawValue match {
        case row: Row =>
          val recordBuilder = ValueRecord.newBuilder()
          val structInfo = dasType.getRecord
          val fieldDefs = structInfo.getAttsList.asScala // each is AttrType

          // For each attribute in the DAS record definition:
          fieldDefs.foreach { att =>
            val fieldName = att.getName
            val fieldType = att.getTipe
            // Attempt to find matching index in Row
            val idx = row.schema.fieldIndex(fieldName)
            val childValue = row.get(idx)

            val nestedVal = rawToProtoValue(childValue, fieldType, s"$colName.$fieldName")
            recordBuilder.addAtts(
              ValueRecordAttr
                .newBuilder()
                .setName(fieldName)
                .setValue(nestedVal))
          }
          return Value.newBuilder().setRecord(recordBuilder.build()).build()

        // Optionally handle map[String, Any] => record if you want that approach
        // I am
        case map: Map[_, _] =>
          val realMap =
            try {
              map.asInstanceOf[Map[String, _]]
            } catch {
              case _: ClassCastException =>
                throw new DASSdkInvalidArgumentException(s"Cannot convert $map to map[String, _] for col=$colName")
            }
          val recordBuilder = ValueRecord.newBuilder()
          val structInfo = dasType.getRecord
          val fieldDefs = structInfo.getAttsList.asScala
          fieldDefs.foreach { att =>
            val fieldName = att.getName
            val fieldType = att.getTipe
            realMap.get(fieldName) match {
              case Some(v) =>
                val nestedVal = rawToProtoValue(v, fieldType, s"$colName.$fieldName")
                recordBuilder.addAtts(ValueRecordAttr.newBuilder().setName(fieldName).setValue(nestedVal))
              case None =>
                // Possibly set null, or skip
                recordBuilder.addAtts(
                  ValueRecordAttr
                    .newBuilder()
                    .setName(fieldName)
                    .setValue(Value.newBuilder().setNull(ValueNull.getDefaultInstance)))
            }
          }
          Value.newBuilder().setRecord(recordBuilder.build()).build()

        case other =>
          throw new DASSdkInvalidArgumentException(s"Cannot convert $other to record type for col=$colName")
      }
    }

    // 4) Optionally, handle decimal, binary, date, etc.
    else if (dasType.hasDecimal) {
      // Spark typically uses java.math.BigDecimal internally
      val decimalStr = rawValue.toString
      Value
        .newBuilder()
        .setDecimal(ValueDecimal.newBuilder().setV(decimalStr))
        .build()
    } else if (dasType.hasBinary) {
      rawValue match {
        case bytes: Array[Byte] =>
          Value
            .newBuilder()
            .setBinary(
              ValueBinary
                .newBuilder()
                .setV(com.google.protobuf.ByteString.copyFrom(bytes)))
            .build()
        case _ =>
          throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to binary ($colName)")
      }
    } else {
      throw new DASSdkInvalidArgumentException(s"Cannot convert ${dasType.getTypeCase} ")

    }
  }

}
