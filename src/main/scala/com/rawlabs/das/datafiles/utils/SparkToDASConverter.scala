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

import org.apache.spark.sql.functions.{col, lit, lower, not}
import org.apache.spark.sql.{Column => SparkColumn, DataFrame, Row, types => sparkTypes}
import org.apache.spark.unsafe.types.CalendarInterval

import com.rawlabs.das.sdk.DASSdkInvalidArgumentException
import com.rawlabs.protocol.das.v1.query.Qual.QualCase
import com.rawlabs.protocol.das.v1.query.{Operator, Qual, SortKey}
import com.rawlabs.protocol.das.v1.types.Type.TypeCase
import com.rawlabs.protocol.das.v1.types.Value.ValueCase
import com.rawlabs.protocol.das.v1.types._

/**
 * A utility object to help convert Spark DataFrames to DAS tables.
 *
 * This object provides functions to:
 *   - Apply filtering (qualifiers) to DataFrames.
 *   - Convert Spark values to DAS protocol values.
 *   - Convert Spark SQL types to DAS types.
 *   - Apply sort keys to DataFrames.
 */
object SparkToDASConverter {

  // -------------------------------------------------------------------
  // Qualifier Pushdown Logic
  // -------------------------------------------------------------------

  /**
   * Applies a sequence of qualifiers to the provided DataFrame.
   *
   * This function iterates over a sequence of DAS qualifiers and applies them as filters on the Spark DataFrame. It
   * supports SIMPLE_QUAL, IS_ALL_QUAL, and IS_ANY_QUAL qualifiers.
   *
   * @param df The input DataFrame to which the qualifiers will be applied.
   * @param quals A sequence of qualifiers specifying filtering conditions.
   * @return A tuple where the first element is the filtered DataFrame and the second is a Boolean flag indicating
   *   whether all qualifiers were successfully applied.
   */
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
          // If no values, then we cannot apply the filter.
          if (values.isEmpty) {
            allApplied = false
          } else {
            val conditions = values
              .map(value => buildFilterCondition(filterCol, isAll.getOperator, value))
            // If any condition is not supported, mark as not fully applied.
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
          // If no values, then we cannot apply the filter.
          if (values.isEmpty) {
            allApplied = false
          } else {
            val conditions = values
              .map(value => buildFilterCondition(filterCol, isAny.getOperator, value))
            // If any condition is not supported, mark as not fully applied.
            if (conditions.contains(None)) {
              allApplied = false
            } else {
              val finalCondition = conditions.map(_.get).reduce(_ || _)
              result = result.filter(finalCondition)
            }
          }
        case QualCase.QUAL_NOT_SET =>
          throw new AssertionError("Qualifier not set")
      }
    }
    (result, allApplied)
  }

  /**
   * Builds a Spark filter condition based on the column, operator, and typed value.
   *
   * This helper function maps a DAS operator to a corresponding Spark SQL Column condition.
   *
   * @param filterCol The Spark Column on which the filter will be applied.
   * @param op The operator to use for comparison (e.g., EQUALS, LESS_THAN).
   * @param typedValue The value to compare against, converted to a native Scala/Java type.
   * @return An Option containing a SparkColumn representing the filter condition, or None if the operator does not
   *   support filtering.
   */
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
      // These operators are supported by Spark but do not make sense to use as filters.
      case Operator.PLUS         => None
      case Operator.MINUS        => None
      case Operator.TIMES        => None
      case Operator.DIV          => None
      case Operator.MOD          => None
      case Operator.OR           => None
      case Operator.AND          => None
      case Operator.UNRECOGNIZED => throw new AssertionError("unrecognized operator")
    }
  }

  // -------------------------------------------------------------------
  // Sorting Utilities
  // -------------------------------------------------------------------

  /**
   * Applies sorting to a DataFrame based on the provided sort keys.
   *
   * For each SortKey, this function constructs a Spark Column with ascending or descending order, as well as optional
   * null ordering (nulls first/last).
   *
   * @param df The DataFrame to be sorted.
   * @param sortKeys A sequence of sort keys defining the column name, sort order, and null ordering.
   * @return The DataFrame sorted according to the specified sort keys.
   */
  def applySortKeys(df: DataFrame, sortKeys: Seq[SortKey]): DataFrame = {
    if (sortKeys.isEmpty) {
      df
    } else {
      val sortCols: Seq[SparkColumn] = sortKeys.map { sk =>
        // Determine sort order and null handling based on the sort key attributes.
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

  // -------------------------------------------------------------------
  // Value Conversion Utilities
  // -------------------------------------------------------------------

  /**
   * Converts a DAS protocol Value to a corresponding Spark native value.
   *
   * Supports various value cases such as:
   *   - NULL
   *   - BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, BOOL, STRING
   *   - BINARY: converts to a byte array
   *   - DATE: converts to java.sql.Date
   *   - TIME: converts to java.sql.Time
   *   - TIMESTAMP: converts to java.sql.Timestamp
   *   - INTERVAL: converts to a Spark CalendarInterval
   *   - RECORD: converts to a Map[String, Any]
   *   - LIST: converts to a Seq[Any]
   *
   * @param value The DAS protocol Value to convert.
   * @return A native Scala/Java value suitable for use in Spark DataFrames.
   */
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
        // Construct a LocalDateTime from the timestamp components.
        val localDateTime = LocalDateTime.of(
          tsVal.getYear,
          tsVal.getMonth,
          tsVal.getDay,
          tsVal.getHour,
          tsVal.getMinute,
          tsVal.getSecond,
          tsVal.getNano)
        // Convert LocalDateTime to java.sql.Timestamp.
        java.sql.Timestamp.valueOf(localDateTime)
      case ValueCase.INTERVAL =>
        val intervalVal = value.getInterval
        // Convert years and months to a total month count.
        val totalMonths = intervalVal.getYears * 12 + intervalVal.getMonths
        // Days are used directly.
        val days = intervalVal.getDays
        // Convert hours, minutes, seconds, and microseconds to total microseconds.
        val totalMicros = {
          val hoursMicros = intervalVal.getHours.toLong * 3_600_000_000L // 3600 sec/hr * 1,000,000 µs/sec
          val minutesMicros = intervalVal.getMinutes.toLong * 60_000_000L // 60 sec/min * 1,000,000 µs/sec
          val secondsMicros = intervalVal.getSeconds.toLong * 1_000_000L
          hoursMicros + minutesMicros + secondsMicros + intervalVal.getMicros
        }
        // Build and return a Spark CalendarInterval.
        new CalendarInterval(totalMonths, days, totalMicros)
      case ValueCase.RECORD =>
        val record = value.getRecord.getAttsList.asScala
        record.map { attr =>
          attr.getName -> protoValueToSparkValue(attr.getValue)
        }.toMap
      case ValueCase.LIST =>
        val list = value.getList.getValuesList.asScala
        list.map(protoValueToSparkValue)
      case ValueCase.VALUE_NOT_SET =>
        throw new AssertionError("Value not set")
    }
  }

  // -------------------------------------------------------------------
  // Schema and Value Mapping Utilities
  // -------------------------------------------------------------------

  /**
   * Converts a Spark SQL DataType to a corresponding DAS type.
   *
   * Supports:
   *   - Basic primitives: Byte, Short, Int, Long, Float, Double, Boolean, String
   *   - DecimalType (converted to DAS Decimal)
   *   - DateType (converted to DAS Date)
   *   - TimestampType (converted to DAS Timestamp)
   *   - BinaryType (converted to DAS Binary)
   *   - ArrayType (converted to DAS ListType)
   *   - StructType (converted to DAS RecordType)
   *   - MapType (converted to a DAS RecordType with "keys" and "values" fields)
   *
   * @param sparkType The Spark SQL DataType to convert.
   * @param nullable Indicates if the field is nullable.
   * @return The corresponding DAS type.
   * @throws DASSdkInvalidArgumentException if the Spark type is unsupported.
   */
  def sparkTypeToDAS(
      sparkType: org.apache.spark.sql.types.DataType,
      nullable: Boolean): com.rawlabs.protocol.das.v1.types.Type = {

    import com.rawlabs.protocol.das.v1.types.{Type => DasType, _}

    sparkType match {
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
      case sparkTypes.BooleanType =>
        DasType
          .newBuilder()
          .setBool(BoolType.newBuilder().setNullable(nullable))
          .build()
      case _: sparkTypes.DecimalType =>
        DasType
          .newBuilder()
          .setDecimal(DecimalType.newBuilder().setNullable(nullable))
          .build()
      case sparkTypes.StringType =>
        DasType
          .newBuilder()
          .setString(StringType.newBuilder().setNullable(nullable))
          .build()
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
      case sparkTypes.BinaryType =>
        DasType
          .newBuilder()
          .setBinary(BinaryType.newBuilder().setNullable(nullable))
          .build()
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
      case sparkTypes.StructType(fields) =>
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
      case sparkTypes.MapType(keyType, valueType, valueContainsNull) =>
        val mapAsRecord = RecordType.newBuilder().setNullable(nullable)
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
      case other =>
        throw new DASSdkInvalidArgumentException(s"Unsupported Spark type: ${other.typeName}")
    }
  }

  /**
   * Recursively converts a raw Spark value into a DAS protocol Value, guided by the specified DAS type.
   *
   * This version uses pattern matching on the DAS type's type case. It handles conversion for:
   *   - Primitive types (String, Int, Long, Bool, Double, Float)
   *   - Arrays/Lists (converted to ValueList)
   *   - Structs/Records (converted to ValueRecord)
   *   - Decimal and Binary types
   *   - Timestamp, Time, and Interval types
   *
   * @param rawValue The raw Spark value to be converted.
   * @param dasType The DAS type definition that guides the conversion.
   * @param colName The name of the column (used for generating error messages).
   * @return The DAS protocol Value representing the raw value.
   * @throws DASSdkInvalidArgumentException if the conversion fails or is unsupported.
   */
  def sparkValueToProtoValue(rawValue: Any, dasType: Type, colName: String): Value = {

    if (rawValue == null) {
      return Value
        .newBuilder()
        .setNull(ValueNull.newBuilder().build())
        .build()
    }

    dasType.getTypeCase match {
      // ---------------------------
      // Primitive types
      // ---------------------------
      case TypeCase.STRING =>
        Value
          .newBuilder()
          .setString(ValueString.newBuilder().setV(rawValue.toString))
          .build()

      case TypeCase.BYTE =>
        val byteVal = rawValue match {
          case b: Byte   => b
          case i: Int    => i.toByte
          case l: Long   => l.toByte
          case s: String => s.toByte
          case _ =>
            throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to byte ($colName)")
        }
        Value
          .newBuilder()
          .setByte(ValueByte.newBuilder().setV(byteVal))
          .build()

      case TypeCase.SHORT =>
        val shortVAl = rawValue match {
          case b: Short  => b
          case i: Int    => i.toShort
          case l: Long   => l.toShort
          case s: String => s.toShort
          case _ =>
            throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to byte ($colName)")
        }
        Value
          .newBuilder()
          .setShort(ValueShort.newBuilder().setV(shortVAl))
          .build()
      case TypeCase.INT =>
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

      case TypeCase.LONG =>
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

      case TypeCase.BOOL =>
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

      case TypeCase.DOUBLE =>
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

      case TypeCase.FLOAT =>
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

      // ---------------------------
      // Temporals
      // ---------------------------
      case TypeCase.TIMESTAMP =>
        rawValue match {
          case ts: java.sql.Timestamp =>
            val ldt = ts.toLocalDateTime
            Value
              .newBuilder()
              .setTimestamp(
                ValueTimestamp
                  .newBuilder()
                  .setYear(ldt.getYear)
                  .setMonth(ldt.getMonthValue)
                  .setDay(ldt.getDayOfMonth)
                  .setHour(ldt.getHour)
                  .setMinute(ldt.getMinute)
                  .setSecond(ldt.getSecond)
                  .setNano(ldt.getNano)
                  .build())
              .build()
          case _ =>
            throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to timestamp ($colName)")
        }

      case TypeCase.DATE =>
        rawValue match {
          case d: java.sql.Date =>
            val ld = d.toLocalDate
            Value
              .newBuilder()
              .setDate(
                ValueDate
                  .newBuilder()
                  .setYear(ld.getYear)
                  .setMonth(ld.getMonthValue)
                  .setDay(ld.getDayOfMonth)
                  .build())
              .build()
          case _ =>
            throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to date ($colName)")
        }

      case TypeCase.TIME =>
        rawValue match {
          case t: java.sql.Time =>
            val lt = t.toLocalTime
            Value
              .newBuilder()
              .setTime(
                ValueTime
                  .newBuilder()
                  .setHour(lt.getHour)
                  .setMinute(lt.getMinute)
                  .setSecond(lt.getSecond)
                  .build())
              .build()
          case _ =>
            throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to time ($colName)")
        }

      case TypeCase.INTERVAL =>
        rawValue match {
          case interval: CalendarInterval =>
            // Decompose the CalendarInterval into its parts.
            val totalMonths = interval.months
            val days = interval.days
            val microsTotal = interval.microseconds
            val years = totalMonths / 12
            val months = totalMonths % 12
            val hours = (microsTotal / 3600000000L).toInt
            val remAfterHours = microsTotal % 3600000000L
            val minutes = (remAfterHours / 60000000L).toInt
            val remAfterMinutes = remAfterHours % 60000000L
            val seconds = (remAfterMinutes / 1000000L).toInt
            val micros = (remAfterMinutes % 1000000L).toInt
            Value
              .newBuilder()
              .setInterval(
                ValueInterval
                  .newBuilder()
                  .setYears(years)
                  .setMonths(months)
                  .setDays(days)
                  .setHours(hours)
                  .setMinutes(minutes)
                  .setSeconds(seconds)
                  .setMicros(micros)
                  .build())
              .build()
          case _ =>
            throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to time ($colName)")
        }

      // ---------------------------
      // Other types
      // ---------------------------
      case TypeCase.DECIMAL =>
        val decimalStr = rawValue.toString
        Value
          .newBuilder()
          .setDecimal(ValueDecimal.newBuilder().setV(decimalStr))
          .build()

      case TypeCase.BINARY =>
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

      // ---------------------------
      // Collection types
      // ---------------------------
      case TypeCase.LIST =>
        val innerType = dasType.getList.getInnerType
        val seq = rawValue match {
          case s: Seq[_]         => s
          case arr: Array[_]     => arr.toSeq
          case iter: Iterable[_] => iter.toSeq
          case other =>
            throw new DASSdkInvalidArgumentException(s"Cannot convert $other to list type for col=$colName")
        }
        val listBuilder = ValueList.newBuilder()
        seq.foreach { elem =>
          val itemValue = sparkValueToProtoValue(elem, innerType, colName + "_elem")
          listBuilder.addValues(itemValue)
        }
        Value
          .newBuilder()
          .setList(listBuilder.build())
          .build()

      case TypeCase.RECORD =>
        rawValue match {
          case row: Row =>
            val recordBuilder = ValueRecord.newBuilder()
            val structInfo = dasType.getRecord
            val fieldDefs = structInfo.getAttsList.asScala
            fieldDefs.foreach { att =>
              val fieldName = att.getName
              val fieldType = att.getTipe
              val idx = row.schema.fieldIndex(fieldName)
              val childValue = row.get(idx)
              val nestedVal = sparkValueToProtoValue(childValue, fieldType, s"$colName.$fieldName")
              recordBuilder.addAtts(ValueRecordAttr.newBuilder().setName(fieldName).setValue(nestedVal))
            }
            Value
              .newBuilder()
              .setRecord(recordBuilder.build())
              .build()

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
                  recordBuilder.addAtts(
                    ValueRecordAttr
                      .newBuilder()
                      .setName(fieldName)
                      .setValue(Value.newBuilder().setNull(ValueNull.getDefaultInstance)))
              }
            }
            Value
              .newBuilder()
              .setRecord(recordBuilder.build())
              .build()

          case other =>
            throw new DASSdkInvalidArgumentException(s"Cannot convert $other to record type for col=$colName")
        }

      case TypeCase.ANY =>
        throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to interval ($colName)")

      case TypeCase.TYPE_NOT_SET =>
        throw new AssertionError("Type not set")
    }

  }

}
