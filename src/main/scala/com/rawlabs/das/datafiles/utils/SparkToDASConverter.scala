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
 * Utility object for converting Spark DataFrames to DAS tables.
 *
 * This object provides functions to:
 *   - Apply filter qualifiers to DataFrames.
 *   - Convert Spark values into DAS protocol values.
 *   - Map Spark SQL types to DAS types.
 *   - Apply sorting based on DAS sort keys.
 */
object SparkToDASConverter {

  // -------------------------------------------------------------------
  // Qualifier Pushdown Logic
  // -------------------------------------------------------------------

  /**
   * Applies a sequence of DAS qualifiers to the given DataFrame.
   *
   * Iterates over each qualifier and applies the corresponding Spark filter. Supported qualifier types are SIMPLE_QUAL,
   * IS_ALL_QUAL, and IS_ANY_QUAL. If a qualifier cannot be applied (for example, if the operator is unsupported for
   * filtering or the qualifier value list is empty), the method sets a flag indicating that not all qualifiers were
   * fully applied.
   *
   * @param df The DataFrame to filter.
   * @param quals A sequence of DAS qualifiers defining the filtering conditions.
   * @return A tuple containing the filtered DataFrame and a Boolean flag that is true if all qualifiers were applied.
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
          // If no values are provided, then the qualifier cannot be applied.
          if (values.isEmpty) {
            allApplied = false
          } else {
            val conditions = values.map(value => buildFilterCondition(filterCol, isAll.getOperator, value))
            // If any condition is unsupported, mark the qualifier as not fully applied.
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
          // If no values are provided, then the qualifier cannot be applied.
          if (values.isEmpty) {
            allApplied = false
          } else {
            val conditions = values.map(value => buildFilterCondition(filterCol, isAny.getOperator, value))
            // If any condition is unsupported, mark the qualifier as not fully applied.
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
   * Constructs a Spark filter condition from a given column, operator, and value.
   *
   * Maps a DAS operator (e.g., EQUALS, LESS_THAN) to a corresponding Spark SQL condition. For operators that are not
   * meaningful for filtering (e.g., arithmetic or logical operators), this method returns None.
   *
   * @param filterCol The Spark column on which the condition will be applied.
   * @param op The DAS operator used for comparison.
   * @param typedValue The value to compare against, in a native Scala/Java type.
   * @return Some(SparkColumn) representing the filter condition, or None if the operator is unsupported.
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
      // Unsupported operators for filtering.
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
   * Applies sorting to a DataFrame based on a sequence of DAS sort keys.
   *
   * For each sort key, a Spark Column is constructed to specify the ordering (ascending or descending) along with the
   * desired null ordering (nulls first or last).
   *
   * @param df The DataFrame to be sorted.
   * @param sortKeys A sequence of sort keys defining the sort order for each column.
   * @return The sorted DataFrame.
   */
  def applySortKeys(df: DataFrame, sortKeys: Seq[SortKey]): DataFrame = {
    if (sortKeys.isEmpty) {
      df
    } else {
      val sortCols: Seq[SparkColumn] = sortKeys.map { sk =>
        // Choose the appropriate sort function based on the sort key attributes.
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
   * Converts a DAS protocol Value into its corresponding native Spark value.
   *
   * Handles conversion for the following DAS value cases:
   *   - NULL, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, BOOL, STRING
   *   - BINARY: converted to a byte array.
   *   - DATE: converted to a java.sql.Date.
   *   - TIME: converted to a java.sql.Time.
   *   - TIMESTAMP: converted to a java.sql.Timestamp.
   *   - INTERVAL: converted to a Spark CalendarInterval.
   *   - RECORD: converted to a Map[String, Any].
   *   - LIST: converted to a Seq[Any].
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
        // Construct a LocalDateTime from timestamp components.
        val localDateTime = LocalDateTime.of(
          tsVal.getYear,
          tsVal.getMonth,
          tsVal.getDay,
          tsVal.getHour,
          tsVal.getMinute,
          tsVal.getSecond,
          tsVal.getNano)
        java.sql.Timestamp.valueOf(localDateTime)
      case ValueCase.INTERVAL =>
        val intervalVal = value.getInterval
        // Convert years and months into total months.
        val totalMonths = intervalVal.getYears * 12 + intervalVal.getMonths
        // Days are used directly.
        val days = intervalVal.getDays
        // Convert hours, minutes, seconds, and microseconds to total microseconds.
        val totalMicros = {
          val hoursMicros = intervalVal.getHours.toLong * 3_600_000_000L
          val minutesMicros = intervalVal.getMinutes.toLong * 60_000_000L
          val secondsMicros = intervalVal.getSeconds.toLong * 1_000_000L
          hoursMicros + minutesMicros + secondsMicros + intervalVal.getMicros
        }
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
   * Converts a Spark SQL DataType into a corresponding DAS type.
   *
   * Supported mappings include:
   *   - Basic primitives: Byte, Short, Int, Long, Float, Double, Boolean, String.
   *   - DecimalType mapped to DAS Decimal.
   *   - DateType mapped to DAS Date.
   *   - TimestampType mapped to DAS Timestamp.
   *   - BinaryType mapped to DAS Binary.
   *   - ArrayType mapped to DAS ListType.
   *   - StructType mapped to DAS RecordType.
   *   - MapType mapped to a DAS RecordType with "keys" and "values" fields.
   *
   * @param sparkType The Spark SQL DataType to convert.
   * @param nullable Indicates whether the type is nullable.
   * @return The corresponding DAS type.
   * @throws DASSdkInvalidArgumentException if the Spark type is unsupported.
   */
  def sparkTypeToDAS(
      sparkType: org.apache.spark.sql.types.DataType,
      nullable: Boolean): com.rawlabs.protocol.das.v1.types.Type = {

    import com.rawlabs.protocol.das.v1.types.{Type => DasType, _}

    sparkType match {
      case sparkTypes.ByteType =>
        DasType.newBuilder().setByte(ByteType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.ShortType =>
        DasType.newBuilder().setShort(ShortType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.IntegerType =>
        DasType.newBuilder().setInt(IntType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.LongType =>
        DasType.newBuilder().setLong(LongType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.FloatType =>
        DasType.newBuilder().setFloat(FloatType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.DoubleType =>
        DasType.newBuilder().setDouble(DoubleType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.BooleanType =>
        DasType.newBuilder().setBool(BoolType.newBuilder().setNullable(nullable)).build()
      case _: sparkTypes.DecimalType =>
        DasType.newBuilder().setDecimal(DecimalType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.StringType =>
        DasType.newBuilder().setString(StringType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.DateType =>
        DasType.newBuilder().setDate(DateType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.TimestampType =>
        DasType.newBuilder().setTimestamp(TimestampType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.BinaryType =>
        DasType.newBuilder().setBinary(BinaryType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.ArrayType(elementType, elementContainsNull) =>
        val elementDasType = sparkTypeToDAS(elementType, elementContainsNull)
        DasType
          .newBuilder()
          .setList(ListType.newBuilder().setInnerType(elementDasType).setNullable(elementContainsNull))
          .build()
      case sparkTypes.StructType(fields) =>
        val attsBuilder = RecordType.newBuilder().setNullable(nullable)
        fields.foreach { field =>
          val dasFieldType = sparkTypeToDAS(field.dataType, field.nullable)
          attsBuilder.addAtts(AttrType.newBuilder().setName(field.name).setTipe(dasFieldType))
        }
        DasType.newBuilder().setRecord(attsBuilder.build()).build()
      case sparkTypes.MapType(keyType, valueType, valueContainsNull) =>
        val mapAsRecord = RecordType.newBuilder().setNullable(nullable)

        val keysType = sparkTypeToDAS(keyType, nullable = false)
        val keysListType =
          DasType.newBuilder().setList(ListType.newBuilder().setInnerType(keysType).setNullable(false)).build()
        mapAsRecord.addAtts(AttrType.newBuilder().setName("keys").setTipe(keysListType))
        val valsType = sparkTypeToDAS(valueType, valueContainsNull)
        val valsListType =
          DasType
            .newBuilder()
            .setList(ListType.newBuilder().setInnerType(valsType).setNullable(valueContainsNull))
            .build()
        mapAsRecord.addAtts(AttrType.newBuilder().setName("values").setTipe(valsListType))
        DasType.newBuilder().setRecord(mapAsRecord.build()).build()
      case other =>
        throw new DASSdkInvalidArgumentException(s"Unsupported Spark type: ${other.typeName}")
    }
  }

  /**
   * Recursively converts a raw Spark value into a DAS protocol Value, based on the provided DAS type.
   *
   * This implementation uses pattern matching on the DAS type's case and supports conversion for:
   *   - Primitive types (String, Byte, Short, Int, Long, Bool, Double, Float).
   *   - Temporal types: Timestamp, Date, and Time.
   *   - Interval types: Converts a Spark CalendarInterval into a DAS interval.
   *   - Collection types: Lists (converted to ValueList).
   *   - Record types: Structs (converted to ValueRecord) or Map[String, _] values.
   *   - Decimal and Binary types.
   *
   * @param rawValue The raw Spark value to convert.
   * @param dasType The DAS type definition guiding the conversion.
   * @param colName The name of the column (used for error messages).
   * @return The corresponding DAS protocol Value.
   * @throws DASSdkInvalidArgumentException if the conversion fails or if the value type is unsupported.
   */
  def sparkValueToProtoValue(rawValue: Any, dasType: Type, colName: String): Value = {

    if (rawValue == null) {
      return Value.newBuilder().setNull(ValueNull.newBuilder().build()).build()
    }

    try {
      dasType.getTypeCase match {
        // ---------------------------
        // Primitive types
        // ---------------------------
        case TypeCase.STRING =>
          Value.newBuilder().setString(ValueString.newBuilder().setV(rawValue.toString)).build()

        case TypeCase.BYTE =>
          val byteVal = rawValue match {
            case b: Byte   => b
            case i: Int    => i.toByte
            case s: Short  => s.toByte
            case l: Long   => l.toByte
            case s: String => s.toByte
            case _ =>
              throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to ${dasType.getTypeCase} ($colName)")
          }
          Value.newBuilder().setByte(ValueByte.newBuilder().setV(byteVal)).build()

        case TypeCase.SHORT =>
          val shortVal = rawValue match {
            case b: Short  => b
            case i: Int    => i.toShort
            case b: Byte   => b.toShort
            case l: Long   => l.toShort
            case s: String => s.toShort
            case _ =>
              throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to ${dasType.getTypeCase} ($colName)")
          }
          Value.newBuilder().setShort(ValueShort.newBuilder().setV(shortVal)).build()

        case TypeCase.INT =>
          val intVal = rawValue match {
            case i: Int    => i
            case l: Long   => l.toInt
            case b: Byte   => b.toInt
            case s: Short  => s.toInt
            case s: String => s.toInt
            case _ =>
              throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to ${dasType.getTypeCase} ($colName)")
          }
          Value.newBuilder().setInt(ValueInt.newBuilder().setV(intVal)).build()

        case TypeCase.LONG =>
          val longVal = rawValue match {
            case l: Long   => l
            case i: Int    => i.toLong
            case b: Byte   => b.toLong
            case s: Short  => s.toLong
            case s: String => s.toLong
            case _ =>
              throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to ${dasType.getTypeCase} ($colName)")
          }
          Value.newBuilder().setLong(ValueLong.newBuilder().setV(longVal)).build()

        case TypeCase.BOOL =>
          val boolVal = rawValue match {
            case b: Boolean => b
            case s: String  => s.toBoolean
            case _ =>
              throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to ${dasType.getTypeCase} ($colName)")
          }
          Value.newBuilder().setBool(ValueBool.newBuilder().setV(boolVal)).build()

        case TypeCase.DOUBLE =>
          val dblVal = rawValue match {
            case d: Double => d
            case f: Float  => f.toDouble
            case s: String => s.toDouble
            case _ =>
              throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to ${dasType.getTypeCase} ($colName)")
          }
          Value.newBuilder().setDouble(ValueDouble.newBuilder().setV(dblVal)).build()

        case TypeCase.FLOAT =>
          val fltVal = rawValue match {
            case f: Float  => f
            case d: Double => d.toFloat
            case s: String => s.toFloat
            case _ =>
              throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to ${dasType.getTypeCase} ($colName)")
          }
          Value.newBuilder().setFloat(ValueFloat.newBuilder().setV(fltVal)).build()

        // ---------------------------
        // Temporal types
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
              throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to ${dasType.getTypeCase} ($colName)")
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
              throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to ${dasType.getTypeCase} ($colName)")
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
              throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to ${dasType.getTypeCase} ($colName)")
          }

        case TypeCase.INTERVAL =>
          rawValue match {
            case interval: CalendarInterval =>
              // Decompose the CalendarInterval into its components.
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
              throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to ${dasType.getTypeCase} ($colName)")
          }

        // ---------------------------
        // Other types
        // ---------------------------
        case TypeCase.DECIMAL =>
          val decimalStr = rawValue.toString
          Value.newBuilder().setDecimal(ValueDecimal.newBuilder().setV(decimalStr)).build()

        case TypeCase.BINARY =>
          rawValue match {
            case bytes: Array[Byte] =>
              Value
                .newBuilder()
                .setBinary(ValueBinary.newBuilder().setV(com.google.protobuf.ByteString.copyFrom(bytes)))
                .build()
            case _ =>
              throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to ${dasType.getTypeCase} ($colName)")
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
          Value.newBuilder().setList(listBuilder.build()).build()

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
              Value.newBuilder().setRecord(recordBuilder.build()).build()

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
              Value.newBuilder().setRecord(recordBuilder.build()).build()

            case other =>
              throw new DASSdkInvalidArgumentException(s"Cannot convert $other to record type for col=$colName")
          }

        case TypeCase.ANY =>
          throw new DASSdkInvalidArgumentException(s"Unsupported conversion for type ANY in column $colName")

        case TypeCase.TYPE_NOT_SET =>
          throw new AssertionError("Type not set")
      }
    } catch {
      case e: NumberFormatException =>
        throw new DASSdkInvalidArgumentException(s"Cannot convert $rawValue to ${dasType.getTypeCase} ($colName)", e)
    }
  }
}
