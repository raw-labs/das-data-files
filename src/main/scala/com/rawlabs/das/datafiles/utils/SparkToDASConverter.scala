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

package com.rawlabs.das.datafiles.utils

import java.time.{LocalDate, LocalDateTime, LocalTime}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.functions.{col, lower, not}
import org.apache.spark.sql.{Column => SparkColumn, DataFrame, Row, types => sparkTypes}
import org.apache.spark.unsafe.types.CalendarInterval

import com.rawlabs.das.sdk.DASSdkInvalidArgumentException
import com.rawlabs.protocol.das.v1.query.Qual.QualCase
import com.rawlabs.protocol.das.v1.query.{Operator, Qual, SortKey}
import com.rawlabs.protocol.das.v1.types.Value.ValueCase
import com.rawlabs.protocol.das.v1.types._

/**
 * A utility object to help convert Spark DataFrames to DAS tables.
 */
object SparkToDASConverter {
  // -------------------------------------------------------------------
  // qualifier pushdown logic
  // -------------------------------------------------------------------
  def applyQuals(df: DataFrame, quals: Seq[Qual]): (DataFrame, Boolean) = {
    var allApplied = true
    var result = df

    for (q <- quals) {
      val colName = q.getName
      val filterCol = col(colName)

      q.getQualCase match {
        case QualCase.SIMPLE_QUAL =>
          val simple = q.getSimpleQual
          val typedValue: Any = protoValueToSparkValue(simple.getValue)

          buildFilterCondition(filterCol, simple.getOperator, typedValue) match {
            case Some(condition) => result = result.filter(condition)
            case None            => allApplied = false
          }
        case QualCase.IS_ALL_QUAL =>
          val isAll = q.getIsAllQual
          val values = isAll.getValuesList.asScala.map(protoValueToSparkValue)
          // if no values, then we can't apply the filter
          if (values.isEmpty) {
            allApplied = false
          } else {
            val conditions = values
              .map(value => buildFilterCondition(filterCol, isAll.getOperator, value))
            // If any of the conditions are None, then we can't apply the filter
            if (conditions.contains(None)) {
              allApplied = false
            } else {
              val finalCondition = conditions.map(_.get).reduce(_ && _)
              result = result.filter(finalCondition)
            }
          }
        case QualCase.IS_ANY_QUAL =>
          val isAny = q.getIsAnyQual
          val values = isAny.getValuesList.asScala.map(protoValueToSparkValue)
          // if no values, then we can't apply the filter
          if (values.isEmpty) {
            allApplied = false
          } else {
            val conditions = values
              .map(value => buildFilterCondition(filterCol, isAny.getOperator, value))
            // If any of the conditions are None, then we can't apply the filter
            if (conditions.contains(None)) {
              allApplied = false
            } else {
              val finalCondition = conditions.map(_.get).reduce(_ || _)
              result = result.filter(finalCondition)
            }
          }
        case QualCase.QUAL_NOT_SET =>
          throw new AssertionError("Qual not set")
      }

    }
    (result, allApplied)
  }

  private def buildFilterCondition(filterCol: SparkColumn, op: Operator, typedValue: Any): Option[SparkColumn] = {
    op match {
      case Operator.EQUALS if typedValue == null =>
        Some(filterCol.isNull)
      case Operator.NOT_EQUALS if typedValue == null =>
        Some(filterCol.isNotNull)
      case Operator.EQUALS =>
        Some(filterCol === typedValue)
      case Operator.NOT_EQUALS =>
        Some(filterCol =!= typedValue)
      case Operator.LESS_THAN =>
        Some(filterCol < typedValue)
      case Operator.LESS_THAN_OR_EQUAL =>
        Some(filterCol <= typedValue)
      case Operator.GREATER_THAN =>
        Some(filterCol > typedValue)
      case Operator.GREATER_THAN_OR_EQUAL =>
        Some(filterCol >= typedValue)
      case Operator.LIKE =>
        Some(filterCol.like(typedValue.toString))
      case Operator.NOT_LIKE =>
        Some(not(filterCol.like(typedValue.toString)))
      case Operator.ILIKE =>
        Some(lower(filterCol).like(typedValue.toString.toLowerCase))
      case Operator.NOT_ILIKE =>
        Some(not(lower(filterCol).like(typedValue.toString.toLowerCase)))
      // These operators are supported by spark, but does it make sense to filter by them?
      case Operator.PLUS  => None
      case Operator.MINUS => None
      case Operator.TIMES => None
      case Operator.DIV   => None
      case Operator.MOD   => None
      case Operator.OR    => None
      case Operator.AND   => None
      // assert here ?
      case Operator.UNRECOGNIZED => None

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
  def applySortKeys(df: DataFrame, sortKeys: Seq[SortKey]): DataFrame = {
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

  def protoValueToSparkValue(value: Value): Any = {
    value.getValueCase match {
      case ValueCase.NULL    => null
      case ValueCase.BYTE    => value.getByte.getV
      case ValueCase.SHORT   => value.getShort.getV
      case ValueCase.INT     => value.getInt.getV
      case ValueCase.LONG    => value.getLong.getV
      case ValueCase.FLOAT   => value.getFloat.getV
      case ValueCase.DOUBLE  => value.getDouble.getV
      case ValueCase.DECIMAL => value.getDecimal.getV
      case ValueCase.BOOL    => value.getBool.getV
      case ValueCase.STRING  => value.getString.getV
      case ValueCase.BINARY  => value.getBinary.getV.toByteArray
      case ValueCase.DATE =>
        val dValue = value.getDate
        val localDate = LocalDate.of(dValue.getYear, dValue.getMonth, dValue.getDay)
        java.sql.Date.valueOf(localDate)
      case ValueCase.TIME =>
        val tValue = value.getTime
        val localTime = LocalTime.of(tValue.getHour, tValue.getMinute, tValue.getSecond)
        java.sql.Time.valueOf(localTime)
      case ValueCase.TIMESTAMP =>
        val tsVal = value.getTimestamp
        // Construct a LocalDateTime
        val localDateTime = LocalDateTime.of(
          tsVal.getYear,
          tsVal.getMonth,
          tsVal.getDay,
          tsVal.getHour,
          tsVal.getMinute,
          tsVal.getSecond,
          tsVal.getNano)
        // Convert to java.sql.Timestamp
        java.sql.Timestamp.valueOf(localDateTime)
      case ValueCase.INTERVAL =>
        val intervalVal = value.getInterval
        // 1. Convert years/months to a total month count
        val totalMonths = intervalVal.getYears * 12 + intervalVal.getMonths

        // 2. Days is stored as is
        val days = intervalVal.getDays

        // 3. Convert hours, minutes, seconds, and micros to total microseconds
        val totalMicros = {
          val hoursMicros = intervalVal.getHours.toLong * 3_600_000_000L // 3600 seconds/hr * 1,000,000 µs/s
          val minutesMicros = intervalVal.getMinutes.toLong * 60_000_000L // 60 seconds/min * 1,000,000 µs/s
          val secondsMicros = intervalVal.getSeconds.toLong * 1_000_000L
          // Add the leftover microseconds
          hoursMicros + minutesMicros + secondsMicros + intervalVal.getMicros
        }

        // 4. Build the Spark CalendarInterval
        new CalendarInterval(totalMonths, days, totalMicros)
      case ValueCase.RECORD =>
        val record = value.getRecord.getAttsList.asScala
        record.map { attr =>
          attr.getName -> protoValueToSparkValue(attr.getValue)
        }.toMap
      case ValueCase.LIST =>
        val list = value.getList.getValuesList.asScala
        list.map(protoValueToSparkValue)
      case ValueCase.VALUE_NOT_SET => throw new AssertionError("Value not set")
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
  def sparkTypeToDAS(
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
  def sparkValueToProtoValue(rawValue: Any, dasType: Type, colName: String): Value = {

    // 0) Null check
    if (rawValue == null) {
      Value
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
        val itemValue = sparkValueToProtoValue(elem, innerType, colName + "_elem")
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

            val nestedVal = sparkValueToProtoValue(childValue, fieldType, s"$colName.$fieldName")
            recordBuilder.addAtts(
              ValueRecordAttr
                .newBuilder()
                .setName(fieldName)
                .setValue(nestedVal))
          }
          Value.newBuilder().setRecord(recordBuilder.build()).build()

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
                val nestedVal = sparkValueToProtoValue(v, fieldType, s"$colName.$fieldName")
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
      throw new DASSdkInvalidArgumentException(s"Cannot convert ${dasType.getTypeCase}")
    }
  }
}
