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

package com.rawlabs.das.datafile

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Column => SparkColumn, DataFrame, Row, types => sparkTypes}

import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.das.sdk.{DASExecuteResult, DASSdkException}
import com.rawlabs.protocol.das.v1.query.{Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.{Column => ProtoColumn, Row => ProtoRow, TableDefinition}
import com.rawlabs.protocol.das.v1.types._

/**
 * An abstract base class for "Data File" tables. Common logic:
 *   - Storing tableName, url, SparkSession
 *   - Overriding basic DASTable methods
 *   - Inferring schema / building TableDefinition (if desired)
 *
 * Child classes implement: def loadDataFrame(): DataFrame
 */
abstract class BaseDataFileTable(val tableName: String, val url: String) extends DASTable {

  def format: String

  def tableDefinition: TableDefinition

  override def getTablePathKeys: Seq[com.rawlabs.protocol.das.v1.query.PathKey] = Seq.empty
  override def getTableSortOrders(sortKeys: Seq[SortKey]): Seq[SortKey] = Seq.empty

  // -------------------------------------------------------------------
  // 1) Infer the schema and columns once, store them
  // -------------------------------------------------------------------
  protected lazy val dfSchema: sparkTypes.StructType = loadDataFrame().schema

  /**
   * Convert the Spark schema to a list of (colName -> DAS Type)
   */
  protected lazy val columns: Seq[(String, Type)] = {
    dfSchema.fields.map { field =>
      val dasType = sparkTypeToDAS(field.dataType, field.nullable)
      (field.name, dasType)
    }
  }

  /**
   * The main data read flow: 1) loadDataFrame() [abstract method implemented by child classes] 2) optionally apply
   * filters (pushdown) 3) select requested columns 4) limit 5) convert to DAS rows
   */
  override def execute(
      quals: Seq[Qual],
      columnsRequested: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long]): DASExecuteResult = {

    val df = loadDataFrame()
    val filteredDF = applyQuals(df, quals)

    val finalCols = if (columnsRequested.nonEmpty) columnsRequested else filteredDF.columns.toSeq
    val dfSelected = filteredDF.select(finalCols.map(c => org.apache.spark.sql.functions.col(c)): _*)

    val dfLimited = maybeLimit match {
      case Some(l) => dfSelected.limit(l.toInt)
      case None    => dfSelected
    }

    val sparkIter = dfLimited.toLocalIterator().asScala

    // For quick lookup of col -> DAS Type
    val colTypesMap: Map[String, Type] = columns.toMap

    new DASExecuteResult {
      override def hasNext: Boolean = sparkIter.hasNext

      override def next(): ProtoRow = {
        val rowBuilder = ProtoRow.newBuilder()
        val row = sparkIter.next()

        finalCols.foreach { col =>
          val rawVal = row.getAs[Any](col)
          val dasType = colTypesMap.getOrElse(col, throw new DASSdkException(s"Column $col not found in schema"))
          val protoVal = rawToProtoValue(rawVal, dasType, col)
          rowBuilder.addColumns(ProtoColumn.newBuilder().setName(col).setData(protoVal))
        }
        rowBuilder.build()
      }

      override def close(): Unit = {}
    }
  }

  // Mark read-only
  override def insert(row: ProtoRow): ProtoRow =
    throw new DASSdkException(s"DataFile table '$tableName' is read-only.")

  override def update(rowId: Value, newRow: ProtoRow): ProtoRow =
    throw new DASSdkException(s"DataFile table '$tableName' is read-only.")

  override def delete(rowId: Value): Unit =
    throw new DASSdkException(s"DataFile table '$tableName' is read-only.")

  // -------------------------------------------------------------------
  // 3) Abstract method(s) children must implement
  // -------------------------------------------------------------------
  /**
   * Child class reads the file with Spark, e.g. spark.read.csv(...).
   */
  protected def loadDataFrame(): DataFrame

  // -------------------------------------------------------------------
  // 4) Optionally, common pushdown logic
  // -------------------------------------------------------------------
  private def applyQuals(df: DataFrame, quals: Seq[Qual]): DataFrame = {
    import org.apache.spark.sql.functions._
    import com.rawlabs.protocol.das.v1.query.Operator

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
            // For example, Spark handles col IS NULL / col IS NOT NULL differently,
            // so "x = null" won't match anything. You might throw or skip.
            throw new DASSdkException(s"Filtering on NULL is not fully supported in base class (col=$colName).")
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
            throw new DASSdkException(s"Unsupported filter type on col=$colName in base class.")
          }
        }

        // Build the Spark filter expression based on the operator
        val condition: SparkColumn = op match {
          case Operator.EQUALS =>
            filterCol === typedValue

          case Operator.NOT_EQUALS =>
            filterCol =!= typedValue

          case Operator.LESS_THAN =>
            filterCol < typedValue

          case Operator.LESS_THAN_OR_EQUAL =>
            filterCol <= typedValue

          case Operator.GREATER_THAN =>
            filterCol > typedValue

          case Operator.GREATER_THAN_OR_EQUAL =>
            filterCol >= typedValue

          case Operator.LIKE =>
            // LIKE requires a string pattern, e.g. '%abc%'
            if (!valProto.hasString) {
              throw new DASSdkException("LIKE operator requires a string value")
            }
            filterCol.like(typedValue.toString)

          case Operator.NOT_LIKE =>
            if (!valProto.hasString) {
              throw new DASSdkException("NOT LIKE operator requires a string value")
            }
            not(filterCol.like(typedValue.toString))

          case Operator.ILIKE =>
            // Spark doesn't have ilike() built-in. We can emulate with lower(...) like(...)
            if (!valProto.hasString) {
              throw new DASSdkException("ILIKE operator requires a string value")
            }
            lower(filterCol).like(typedValue.toString.toLowerCase)

          case Operator.NOT_ILIKE =>
            if (!valProto.hasString) {
              throw new DASSdkException("NOT ILIKE operator requires a string value")
            }
            not(lower(filterCol).like(typedValue.toString.toLowerCase))

          // You may optionally handle more operators if your proto includes them:
          //   PLUS, MINUS, TIMES, DIV, MOD, AND, OR, etc.
          // Typically these are not direct filter operators.
          // If desired, handle them here or skip/throw:
          case other =>
            throw new DASSdkException(s"Operator $other not supported in base class.")
        }

        // Apply the filter to the running result
        result = result.filter(condition)
      }
    }

    result
  }

  // -------------------------------------------------------------------
  // 5) Utilities for schema mapping, building proto Values, etc.
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
      // 10) Fallback => String
      // -----------------------------------------
      case other =>
        throw new DASSdkException(s"Unsupported Spark type: ${other.typeName}")
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
    if (dasType.hasString) {
      return Value
        .newBuilder()
        .setString(ValueString.newBuilder().setV(rawValue.toString))
        .build()
    } else if (dasType.hasInt) {
      val intVal = rawValue match {
        case i: Int    => i
        case l: Long   => l.toInt
        case s: String => s.toInt
        case _ =>
          throw new DASSdkException(s"Cannot convert $rawValue to int ($colName)")
      }
      return Value
        .newBuilder()
        .setInt(ValueInt.newBuilder().setV(intVal))
        .build()
    } else if (dasType.hasLong) {
      val longVal = rawValue match {
        case l: Long   => l
        case i: Int    => i.toLong
        case s: String => s.toLong
        case _ =>
          throw new DASSdkException(s"Cannot convert $rawValue to long ($colName)")
      }
      return Value
        .newBuilder()
        .setLong(ValueLong.newBuilder().setV(longVal))
        .build()
    } else if (dasType.hasBool) {
      val boolVal = rawValue match {
        case b: Boolean => b
        case s: String  => s.toBoolean
        case _ =>
          throw new DASSdkException(s"Cannot convert $rawValue to bool ($colName)")
      }
      return Value
        .newBuilder()
        .setBool(ValueBool.newBuilder().setV(boolVal))
        .build()
    } else if (dasType.hasDouble) {
      val dblVal = rawValue match {
        case d: Double => d
        case f: Float  => f.toDouble
        case s: String => s.toDouble
        case _ =>
          throw new DASSdkException(s"Cannot convert $rawValue to double ($colName)")
      }
      return Value
        .newBuilder()
        .setDouble(ValueDouble.newBuilder().setV(dblVal))
        .build()
    } else if (dasType.hasFloat) {
      val fltVal = rawValue match {
        case f: Float  => f
        case d: Double => d.toFloat
        case s: String => s.toFloat
        case _ =>
          throw new DASSdkException(s"Cannot convert $rawValue to float ($colName)")
      }
      return Value
        .newBuilder()
        .setFloat(ValueFloat.newBuilder().setV(fltVal))
        .build()
    }

    // 2) If we have a list/array type => ValueList
    if (dasType.hasList) {
      val innerType = dasType.getList.getInnerType
      val seq = rawValue match {
        case s: Seq[_]         => s
        case arr: Array[_]     => arr.toSeq
        case iter: Iterable[_] => iter.toSeq
        // Spark often uses WrappedArray for arrays
        case other =>
          throw new DASSdkException(s"Cannot convert $other to list type for col=$colName")
      }

      val listBuilder = ValueList.newBuilder()
      seq.foreach { elem =>
        val itemValue = rawToProtoValue(elem, innerType, colName + "_elem")
        listBuilder.addValues(itemValue)
      }
      return Value.newBuilder().setList(listBuilder.build()).build()
    }

    // 3) If we have a record/struct => ValueRecord
    if (dasType.hasRecord) {
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
                throw new DASSdkException(s"Cannot convert $map to map[String, _] for col=$colName")
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
          return Value.newBuilder().setRecord(recordBuilder.build()).build()

        case other =>
          throw new DASSdkException(s"Cannot convert $other to record type for col=$colName")
      }
    }

    // 4) Optionally, handle decimal, binary, date, etc.
    if (dasType.hasDecimal) {
      // Spark typically uses java.math.BigDecimal internally
      val decimalStr = rawValue.toString
      return Value
        .newBuilder()
        .setDecimal(ValueDecimal.newBuilder().setV(decimalStr))
        .build()
    }
    if (dasType.hasBinary) {
      rawValue match {
        case bytes: Array[Byte] =>
          return Value
            .newBuilder()
            .setBinary(
              ValueBinary
                .newBuilder()
                .setV(com.google.protobuf.ByteString.copyFrom(bytes)))
            .build()
        case _ =>
          throw new DASSdkException(s"Cannot convert $rawValue to binary ($colName)")
      }
    }

    // 5) Fallback to string if none of the above matched
    return Value
      .newBuilder()
      .setString(ValueString.newBuilder().setV(rawValue.toString))
      .build()
  }

}
