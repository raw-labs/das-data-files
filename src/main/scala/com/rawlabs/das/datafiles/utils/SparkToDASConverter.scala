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
  def applyQuals(df: DataFrame, quals: Seq[Qual], colTypes: Map[String, sparkTypes.DataType]): (DataFrame, Boolean) = {
    var allApplied = true
    var result = df

    for (q <- quals) {
      val colName = q.getName
      val filterCol = col(colName)
      val sparkType = colTypes(colName)
      // Retrieve the Spark DataType for this column
      q.getQualCase match {
        case QualCase.SIMPLE_QUAL =>
          val simple = q.getSimpleQual
          toComparableSparkValue(simple.getValue, sparkType) match {
            case Some(typedValue) =>
              buildFilterCondition(filterCol, simple.getOperator, typedValue) match {
                case Some(condition) => result = result.filter(condition)
                case None            => allApplied = false
              }
            case None =>
              allApplied = false
          }
        case QualCase.IS_ALL_QUAL =>
          val isAll = q.getIsAllQual
          val convertedValues = isAll.getValuesList.asScala.map { v =>
            toComparableSparkValue(v, sparkType)
          }.toSeq
          // If no values or any conversion fails, mark qualifier as not applied.
          if (convertedValues.isEmpty || convertedValues.exists(_.isEmpty)) {
            allApplied = false
          } else {
            val conditions = convertedValues.map(_.get).map { v =>
              buildFilterCondition(filterCol, isAll.getOperator, v)
            }
            if (conditions.contains(None)) {
              allApplied = false
            } else {
              val finalCondition = conditions.map(_.get).reduce(_ && _)
              result = result.filter(finalCondition)
            }
          }
        case QualCase.IS_ANY_QUAL =>
          val isAny = q.getIsAnyQual
          val convertedValues = isAny.getValuesList.asScala.map { v =>
            toComparableSparkValue(v, sparkType)
          }.toSeq
          // If no values or any conversion fails, mark qualifier as not applied.
          if (convertedValues.isEmpty || convertedValues.exists(_.isEmpty)) {
            allApplied = false
          } else {
            val conditions = convertedValues.map(_.get).map { v =>
              buildFilterCondition(filterCol, isAny.getOperator, v)
            }
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
   * Converts a DAS protocol Value into its corresponding native Spark value,
   * using the provided Spark SQL DataType.
   *
   * This function is intended to be used only in the context of filtering qualifiers.
   * It attempts to convert the given DAS value (such as INT, STRING, DATE, etc.) into
   * a Spark value that can be compared in a filter condition. The conversion is performed
   * based on the provided Spark type.
   *
   * If the conversion is successful, the resulting Spark value is wrapped in a Some(value).
   * If the conversion is not possible (for example, if the types do not match or the conversion
   * is unsupported), the function returns None. In the context of qualifier pushdown,
   * a None indicates that the qualifier cannot be applied.
   * 
   * @param value The DAS protocol Value to be converted.
   * @param sparkType The Spark SQL DataType that the output value should conform to.
   * @return An Option[Any] containing the native Spark value if the conversion is successful,
   *         or None if the conversion is not possible (indicating that the qualifier cannot be applied).
   */
  def toComparableSparkValue(value: Value, sparkType: sparkTypes.DataType): Option[Any] = {
    (value.getValueCase, sparkType) match {
      case (ValueCase.NULL, _) =>
        Some(null)
      case (ValueCase.BYTE, _) =>
        Some(value.getByte.getV)
      case (ValueCase.SHORT, _) =>
        Some(value.getShort.getV)
      case (ValueCase.INT, _) =>
        Some(value.getInt.getV)
      case (ValueCase.LONG, _) =>
        Some(value.getLong.getV)
      case (ValueCase.FLOAT, _) =>
        Some(value.getFloat.getV)
      case (ValueCase.DOUBLE, _) =>
        Some(value.getDouble.getV)
      case (ValueCase.DECIMAL, _) =>
        Some(value.getDecimal.getV)
      case (ValueCase.BOOL, _) =>
        Some(value.getBool.getV)
      case (ValueCase.STRING, _) =>
        Some(value.getString.getV)
      case (ValueCase.BINARY, _) =>
        Some(value.getBinary.getV.toByteArray)
      case (ValueCase.DATE, _) =>
        val dValue = value.getDate
        val localDate = LocalDate.of(dValue.getYear, dValue.getMonth, dValue.getDay)
        Some(java.sql.Date.valueOf(localDate))
      case (ValueCase.TIME, _) =>
        val tValue = value.getTime
        val localTime = LocalTime.of(tValue.getHour, tValue.getMinute, tValue.getSecond)
        Some(java.sql.Time.valueOf(localTime))
      case (ValueCase.TIMESTAMP, _) =>
        val tsVal = value.getTimestamp
        val localDateTime = LocalDateTime.of(
          tsVal.getYear,
          tsVal.getMonth,
          tsVal.getDay,
          tsVal.getHour,
          tsVal.getMinute,
          tsVal.getSecond,
          tsVal.getNano)
        Some(java.sql.Timestamp.valueOf(localDateTime))
      case (ValueCase.INTERVAL, _) =>
        val intervalVal = value.getInterval
        val totalMonths = intervalVal.getYears * 12 + intervalVal.getMonths
        val days = intervalVal.getDays
        val totalMicros = {
          val hoursMicros = intervalVal.getHours.toLong * 3_600_000_000L
          val minutesMicros = intervalVal.getMinutes.toLong * 60_000_000L
          val secondsMicros = intervalVal.getSeconds.toLong * 1_000_000L
          hoursMicros + minutesMicros + secondsMicros + intervalVal.getMicros
        }
        Some(new CalendarInterval(totalMonths, days, totalMicros))

      // In the RECORD case we assume the Spark type is a StructType.
      case (ValueCase.RECORD, struct: sparkTypes.StructType) =>
        val recordFields = value.getRecord.getAttsList.asScala
        // For each field in the Spark StructType, find the matching attribute and recursively convert it.
        val fieldMap: Map[String, Option[Any]] = struct.fields.map { field =>
          val attrOpt = recordFields.find(_.getName == field.name)
          field.name -> attrOpt.flatMap(attr => toComparableSparkValue(attr.getValue, field.dataType))
        }.toMap
        // If any field conversion failed (i.e. returned None), we fail the whole conversion.
        if (fieldMap.values.exists(_.isEmpty)) None
        else Some(fieldMap.map(x => x._1 -> x._2.get))

      // For LIST we assume the Spark type is an ArrayType.
      case (ValueCase.LIST, sparkTypes.ArrayType(elementType, _)) =>
        val listValues = value.getList.getValuesList.asScala.map(v => toComparableSparkValue(v, elementType))
        if (listValues.exists(_.isEmpty)) None else Some(listValues.flatten.toSeq)
      // For any unmatched combination (or unsupported type) we return None.
      case _ => None
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
  def sparkTypeToDAS(sparkType: sparkTypes.DataType, nullable: Boolean): com.rawlabs.protocol.das.v1.types.Type = {

    sparkType match {
      // Numeric types
      case sparkTypes.ByteType =>
        Type.newBuilder().setByte(ByteType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.ShortType =>
        Type.newBuilder().setShort(ShortType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.IntegerType =>
        Type.newBuilder().setInt(IntType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.LongType =>
        Type.newBuilder().setLong(LongType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.FloatType =>
        Type.newBuilder().setFloat(FloatType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.DoubleType =>
        Type.newBuilder().setDouble(DoubleType.newBuilder().setNullable(nullable)).build()
      case _: sparkTypes.DecimalType =>
        Type.newBuilder().setDecimal(DecimalType.newBuilder().setNullable(nullable)).build()
      // Boolean and String types
      case sparkTypes.BooleanType =>
        Type.newBuilder().setBool(BoolType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.StringType | sparkTypes.VarcharType(_) =>
        Type.newBuilder().setString(StringType.newBuilder().setNullable(nullable)).build()
      // Binary types
      case sparkTypes.BinaryType =>
        Type.newBuilder().setBinary(BinaryType.newBuilder().setNullable(nullable)).build()
      // Temporal types
      case _: sparkTypes.CalendarIntervalType | _: sparkTypes.DayTimeIntervalType |
          _: sparkTypes.YearMonthIntervalType =>
        Type.newBuilder().setInterval(IntervalType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.DateType =>
        Type.newBuilder().setDate(DateType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.TimestampNTZType =>
        Type.newBuilder().setTimestamp(TimestampType.newBuilder().setNullable(nullable)).build()
      // TODO: Handle time zones
      case sparkTypes.TimestampType =>
        Type.newBuilder().setTimestamp(TimestampType.newBuilder().setNullable(nullable)).build()
      // Collection Types
      case sparkTypes.ArrayType(elementType, elementContainsNull) =>
        val elementDasType = sparkTypeToDAS(elementType, elementContainsNull)
        Type
          .newBuilder()
          .setList(ListType.newBuilder().setInnerType(elementDasType).setNullable(elementContainsNull))
          .build()
      case sparkTypes.StructType(fields) =>
        val attsBuilder = RecordType.newBuilder().setNullable(nullable)
        fields.foreach { field =>
          val dasFieldType = sparkTypeToDAS(field.dataType, field.nullable)
          attsBuilder.addAtts(AttrType.newBuilder().setName(field.name).setTipe(dasFieldType))
        }
        Type.newBuilder().setRecord(attsBuilder.build()).build()
      // MapType string -> _ is represented as a RecordType with an unknown number of fields.
      case sparkTypes.MapType(_: sparkTypes.StringType, _, _) =>
        Type.newBuilder().setRecord(RecordType.newBuilder().setNullable(nullable)).build()
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
