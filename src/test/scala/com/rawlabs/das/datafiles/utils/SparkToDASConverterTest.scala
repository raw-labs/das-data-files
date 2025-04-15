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

import java.sql.{Date, Time, Timestamp}
import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{types => sparkTypes}
import org.apache.spark.unsafe.types.CalendarInterval
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.datafiles.SparkTestContext
import com.rawlabs.das.sdk.DASSdkInvalidArgumentException
import com.rawlabs.protocol.das.v1.query._
import com.rawlabs.protocol.das.v1.types.Value.ValueCase
import com.rawlabs.protocol.das.v1.{types => dasTypes}

/**
 * Unit tests for SparkToDASConverter object methods: 1) applyQuals 2) applySortKeys 3) sparkTypeToDAS 4)
 * sparkValueToProtoValue 5) toComparableSparkValue (formerly protoValueToSparkValue)
 */
class SparkToDASConverterTest extends AnyFlatSpec with Matchers with SparkTestContext {

  // --------------------------------------------------------------------------
  // 1) applyQuals
  // --------------------------------------------------------------------------
  behavior of "SparkToDASConverter.applyQuals"

  it should "filter rows by EQUALS on a numeric column" in {
    import spark.implicits._
    val df = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    val colTypes = df.schema.fields.map(f => f.name -> f.dataType).toMap

    // Build a Qual => "id = 2"
    val qual = Qual
      .newBuilder()
      .setName("id")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(dasTypes.Value.newBuilder().setInt(dasTypes.ValueInt.newBuilder().setV(2))))
      .build()

    val (filtered, allApplied) = SparkToDASConverter.applyQuals(df, Seq(qual), colTypes)
    allApplied shouldBe true
    val rows = filtered.collect()
    rows.length shouldBe 1
    rows.head.getInt(0) shouldBe 2
    rows.head.getString(1) shouldBe "Bob"
  }

  it should "handle an IS NULL qualifier" in {
    import spark.implicits._
    val df = Seq((1, "hello"), (2, null.asInstanceOf[String])).toDF("id", "text")
    val colTypes = df.schema.fields.map(f => f.name -> f.dataType).toMap

    // "text = null" => text.isNull
    val qual = Qual
      .newBuilder()
      .setName("text")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(dasTypes.Value.newBuilder().setNull(dasTypes.ValueNull.newBuilder().build())))
      .build()

    val (filtered, allApplied) = SparkToDASConverter.applyQuals(df, Seq(qual), colTypes)
    allApplied shouldBe true
    val rows = filtered.collect()
    rows.length shouldBe 1
    rows.head.getInt(0) shouldBe 2
    rows.head.isNullAt(1) shouldBe true
  }

  it should "handle a LIKE on a string column" in {
    import spark.implicits._
    val df = Seq((1, "alice@domain.com"), (2, "bob@domain.com"), (3, "none")).toDF("id", "email")
    val colTypes = df.schema.fields.map(f => f.name -> f.dataType).toMap

    // "email LIKE '%@domain.com'"
    val qual = Qual
      .newBuilder()
      .setName("email")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.LIKE)
          .setValue(dasTypes.Value.newBuilder().setString(dasTypes.ValueString.newBuilder().setV("%@domain.com"))))
      .build()

    val (filtered, allApplied) = SparkToDASConverter.applyQuals(df, Seq(qual), colTypes)
    allApplied shouldBe true
    val rows = filtered.collect()
    rows.map(_.getInt(0)).sorted shouldBe Array(1, 2)
  }

  it should "mark qualifier as not fully applied when using unsupported operator" in {
    import spark.implicits._
    val df = Seq((1, "A"), (2, "B")).toDF("id", "letter")
    val colTypes = df.schema.fields.map(f => f.name -> f.dataType).toMap

    // Create a qualifier with operator PLUS which is unsupported for filtering.
    val qual = Qual
      .newBuilder()
      .setName("id")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.PLUS)
          .setValue(dasTypes.Value.newBuilder().setInt(dasTypes.ValueInt.newBuilder().setV(1))))
      .build()
    val (filtered, allApplied) = SparkToDASConverter.applyQuals(df, Seq(qual), colTypes)
    filtered.count() shouldBe df.count()
    allApplied shouldBe false
  }

  it should "mark qualifier as not fully applied for IS_ALL_QUAL with empty value list" in {
    import spark.implicits._
    val df = Seq((1, "A"), (2, "B")).toDF("id", "letter")
    val colTypes = df.schema.fields.map(f => f.name -> f.dataType).toMap

    val qual = Qual
      .newBuilder()
      .setName("id")
      .setIsAllQual(
        IsAllQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          // No values added: empty list
      )
      .build()
    val (filtered, allApplied) = SparkToDASConverter.applyQuals(df, Seq(qual), colTypes)
    filtered.count() shouldBe df.count()
    allApplied shouldBe false
  }

  it should "mark qualifier as not fully applied for IS_ANY_QUAL with empty value list" in {
    import spark.implicits._
    val df = Seq((1, "A"), (2, "B")).toDF("id", "letter")
    val colTypes = df.schema.fields.map(f => f.name -> f.dataType).toMap

    val qual = Qual
      .newBuilder()
      .setName("id")
      .setIsAnyQual(
        IsAnyQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          // No values added: empty list
      )
      .build()
    val (filtered, allApplied) = SparkToDASConverter.applyQuals(df, Seq(qual), colTypes)
    filtered.count() shouldBe df.count()
    allApplied shouldBe false
  }

  it should "apply IS_ANY_QUAL correctly" in {
    import spark.implicits._
    val df = Seq((1, "A"), (2, "B"), (3, "C")).toDF("id", "letter")
    val colTypes = df.schema.fields.map(f => f.name -> f.dataType).toMap

    // Qualifier: id equals 2 or 3
    val isAnyQual = IsAnyQual
      .newBuilder()
      .setOperator(Operator.EQUALS)
      .addValues(dasTypes.Value.newBuilder().setInt(dasTypes.ValueInt.newBuilder().setV(2)))
      .addValues(dasTypes.Value.newBuilder().setInt(dasTypes.ValueInt.newBuilder().setV(3)))
      .build()
    val qual = Qual.newBuilder().setName("id").setIsAnyQual(isAnyQual).build()
    val (filtered, allApplied) = SparkToDASConverter.applyQuals(df, Seq(qual), colTypes)
    allApplied shouldBe true
    val rows = filtered.collect()
    rows.length shouldBe 2
    rows.map(_.getInt(0)).sorted shouldBe Array(2, 3)
  }

  it should "apply IS_ALL_QUAL resulting in no rows when conditions are mutually exclusive" in {
    import spark.implicits._
    val df = Seq((1, "A"), (2, "B"), (3, "C")).toDF("id", "letter")
    val colTypes = df.schema.fields.map(f => f.name -> f.dataType).toMap

    // Qualifier: id equals 2 and id equals 3 (impossible to satisfy simultaneously)
    val isAllQual = IsAllQual
      .newBuilder()
      .setOperator(Operator.EQUALS)
      .addValues(dasTypes.Value.newBuilder().setInt(dasTypes.ValueInt.newBuilder().setV(2)))
      .addValues(dasTypes.Value.newBuilder().setInt(dasTypes.ValueInt.newBuilder().setV(3)))
      .build()
    val qual = Qual.newBuilder().setName("id").setIsAllQual(isAllQual).build()
    val (filtered, allApplied) = SparkToDASConverter.applyQuals(df, Seq(qual), colTypes)
    filtered.count() shouldBe 0
    // Even if no row qualifies, the qualifier was applied so allApplied should be true.
    allApplied shouldBe true
  }

  // --------------------------------------------------------------------------
  // 2) applySortKeys
  // --------------------------------------------------------------------------
  behavior of "SparkToDASConverter.applySortKeys"

  it should "sort rows by a numeric column descending, nulls first" in {
    import spark.implicits._
    val df =
      Seq((1, "Alice"), (3, null.asInstanceOf[String]), (2, "Bob"), (10, null.asInstanceOf[String])).toDF("id", "name")
    // SortKey => "id DESC NULLS FIRST"
    val sortKey = SortKey
      .newBuilder()
      .setName("id")
      .setIsReversed(true)
      .setNullsFirst(true)
      .build()

    val sortedDF = SparkToDASConverter.applySortKeys(df, Seq(sortKey))
    val rows = sortedDF.collect()

    // We have no null "id" in this sample, but we'll just confirm descending by id:
    rows.map(r => r.getInt(0)) shouldBe Array(10, 3, 2, 1)
  }

  it should "sort rows by multiple sort keys" in {
    import spark.implicits._
    val df = Seq((1, "B", 100), (2, "A", 200), (3, "B", 150), (4, "A", 50)).toDF("id", "group", "value")
    // First sort by group ascending, then by value descending
    val sortKey1 = SortKey.newBuilder().setName("group").setIsReversed(false).setNullsFirst(false).build()
    val sortKey2 = SortKey.newBuilder().setName("value").setIsReversed(true).setNullsFirst(false).build()
    val sortedDF = SparkToDASConverter.applySortKeys(df, Seq(sortKey1, sortKey2))
    val rows = sortedDF.collect()
    // Expected order: group A: (2, "A", 200), (4, "A", 50); group B: (3, "B", 150), (1, "B", 100)
    rows.map(r => (r.getInt(0), r.getString(1), r.getInt(2))) shouldBe Array(
      (2, "A", 200),
      (4, "A", 50),
      (3, "B", 150),
      (1, "B", 100))
  }

  // --------------------------------------------------------------------------
  // 3) sparkTypeToDAS
  // --------------------------------------------------------------------------
  behavior of "SparkToDASConverter.sparkTypeToDAS"

  it should "convert basic spark types to DAS types" in {
    SparkToDASConverter.sparkTypeToDAS(sparkTypes.IntegerType, nullable = true).hasInt shouldBe true
    SparkToDASConverter.sparkTypeToDAS(sparkTypes.LongType, nullable = false).hasLong shouldBe true
    SparkToDASConverter.sparkTypeToDAS(sparkTypes.StringType, nullable = true).hasString shouldBe true
    SparkToDASConverter.sparkTypeToDAS(sparkTypes.BooleanType, nullable = true).hasBool shouldBe true
    SparkToDASConverter.sparkTypeToDAS(sparkTypes.DoubleType, nullable = true).hasDouble shouldBe true
    SparkToDASConverter.sparkTypeToDAS(sparkTypes.FloatType, nullable = false).hasFloat shouldBe true
    SparkToDASConverter.sparkTypeToDAS(sparkTypes.ShortType, nullable = true).hasShort shouldBe true
    SparkToDASConverter.sparkTypeToDAS(sparkTypes.ByteType, nullable = false).hasByte shouldBe true
  }

  it should "convert array, struct, and map spark types" in {
    val arrType = sparkTypes.ArrayType(sparkTypes.StringType, containsNull = true)
    val dasArr = SparkToDASConverter.sparkTypeToDAS(arrType, nullable = false)
    dasArr.hasList shouldBe true
    dasArr.getList.getInnerType.hasString shouldBe true
    dasArr.getList.getInnerType.getString.getNullable shouldBe true

    val structType = sparkTypes.StructType(
      Seq(
        sparkTypes.StructField("id", sparkTypes.IntegerType, nullable = false),
        sparkTypes.StructField("name", sparkTypes.StringType, nullable = true)))
    val dasStruct = SparkToDASConverter.sparkTypeToDAS(structType, nullable = true)
    dasStruct.hasRecord shouldBe true
    dasStruct.getRecord.getAttsCount shouldBe 2

    val mapType = sparkTypes.MapType(sparkTypes.StringType, sparkTypes.IntegerType, valueContainsNull = false)
    val dasMap = SparkToDASConverter.sparkTypeToDAS(mapType, nullable = true)
    dasMap.hasRecord shouldBe true
    // record type with 0 attributes is a map string -> Any
    dasMap.getRecord.getAttsCount shouldBe 0
  }

  // --------------------------------------------------------------------------
  // 4) sparkValueToProtoValue
  // --------------------------------------------------------------------------
  behavior of "SparkToDASConverter.sparkValueToProtoValue"

  it should "convert primitive raw values to the correct proto" in {
    val intType = dasTypes.Type.newBuilder().setInt(dasTypes.IntType.newBuilder().setNullable(false)).build()
    val intVal = SparkToDASConverter.sparkValueToProtoValue(123, intType, "testCol")
    intVal.getInt.getV shouldBe 123

    val boolType = dasTypes.Type.newBuilder().setBool(dasTypes.BoolType.newBuilder().setNullable(true)).build()
    val boolVal = SparkToDASConverter.sparkValueToProtoValue(true, boolType, "colBool")
    boolVal.getBool.getV shouldBe true

    val strType = dasTypes.Type.newBuilder().setString(dasTypes.StringType.newBuilder().setNullable(true)).build()
    val strVal = SparkToDASConverter.sparkValueToProtoValue("Hello", strType, "colString")
    strVal.getString.getV shouldBe "Hello"
  }

  it should "handle null rawValue" in {
    val strType = dasTypes.Type.newBuilder().setString(dasTypes.StringType.newBuilder().setNullable(true)).build()
    val nullVal = SparkToDASConverter.sparkValueToProtoValue(null, strType, "colName")
    nullVal.getValueCase shouldBe ValueCase.NULL
  }

  it should "convert array values to ValueList" in {
    val listType = dasTypes.Type
      .newBuilder()
      .setList(
        dasTypes.ListType
          .newBuilder()
          .setInnerType(dasTypes.Type.newBuilder().setDouble(dasTypes.DoubleType.newBuilder().setNullable(true)))
          .setNullable(true))
      .build()

    val rawSeq = Seq(1.1, 2.2, 3.3)
    val protoVal = SparkToDASConverter.sparkValueToProtoValue(rawSeq, listType, "arrCol")
    protoVal.getList.getValuesCount shouldBe 3
    protoVal.getList.getValues(0).getDouble.getV shouldBe 1.1 +- 1e-6
    protoVal.getList.getValues(1).getDouble.getV shouldBe 2.2 +- 1e-6
    protoVal.getList.getValues(2).getDouble.getV shouldBe 3.3 +- 1e-6
  }

  it should "convert struct row to ValueRecord" in {
    val schema = sparkTypes.StructType(
      Seq(sparkTypes.StructField("id", sparkTypes.IntegerType), sparkTypes.StructField("name", sparkTypes.StringType)))
    val row = new GenericRowWithSchema(Array[Any](42, "FortyTwo"), schema)
    val recordType = dasTypes.Type
      .newBuilder()
      .setRecord(
        dasTypes.RecordType
          .newBuilder()
          .addAtts(
            dasTypes.AttrType
              .newBuilder()
              .setName("id")
              .setTipe(dasTypes.Type.newBuilder().setInt(dasTypes.IntType.newBuilder().setNullable(true))))
          .addAtts(dasTypes.AttrType
            .newBuilder()
            .setName("name")
            .setTipe(dasTypes.Type.newBuilder().setString(dasTypes.StringType.newBuilder().setNullable(true))))
          .setNullable(true))
      .build()

    val protoVal = SparkToDASConverter.sparkValueToProtoValue(row, recordType, "structCol")

    val record = protoVal.getRecord
    record.getAttsCount shouldBe 2
    record.getAtts(0).getName shouldBe "id"
    record.getAtts(0).getValue.getInt.getV shouldBe 42
    record.getAtts(1).getName shouldBe "name"
    record.getAtts(1).getValue.getString.getV shouldBe "FortyTwo"
  }

  it should "convert a Map to ValueRecord with missing fields set to null" in {
    val recordType = dasTypes.Type
      .newBuilder()
      .setRecord(
        dasTypes.RecordType
          .newBuilder()
          .addAtts(
            dasTypes.AttrType
              .newBuilder()
              .setName("id")
              .setTipe(dasTypes.Type.newBuilder().setInt(dasTypes.IntType.newBuilder().setNullable(true))))
          .addAtts(dasTypes.AttrType
            .newBuilder()
            .setName("name")
            .setTipe(dasTypes.Type.newBuilder().setString(dasTypes.StringType.newBuilder().setNullable(true))))
          .setNullable(true))
      .build()
    val mapVal: Map[String, Any] = Map("id" -> 42) // "name" missing
    val protoVal = SparkToDASConverter.sparkValueToProtoValue(mapVal, recordType, "mapCol")
    val record = protoVal.getRecord
    record.getAttsCount shouldBe 2
    record.getAtts(0).getName shouldBe "id"
    record.getAtts(0).getValue.getInt.getV shouldBe 42
    record.getAtts(1).getName shouldBe "name"
    record.getAtts(1).getValue.getValueCase shouldBe ValueCase.NULL
  }

  it should "convert a decimal and binary value correctly" in {
    // decimal
    val decType = dasTypes.Type.newBuilder().setDecimal(dasTypes.DecimalType.newBuilder().setNullable(true)).build()
    val decVal = SparkToDASConverter.sparkValueToProtoValue(new java.math.BigDecimal("123.45"), decType, "colDecimal")
    decVal.getDecimal.getV shouldBe "123.45"

    // binary
    val binType = dasTypes.Type.newBuilder().setBinary(dasTypes.BinaryType.newBuilder().setNullable(false)).build()
    val bytes = "hello".getBytes("UTF-8")
    val binVal = SparkToDASConverter.sparkValueToProtoValue(bytes, binType, "colBinary")
    binVal.getBinary.getV.toByteArray shouldBe bytes
  }

  it should "throw an exception if raw value doesn't match the expected type" in {
    val intType = dasTypes.Type.newBuilder().setInt(dasTypes.IntType.newBuilder().setNullable(false)).build()
    intercept[DASSdkInvalidArgumentException] {
      SparkToDASConverter.sparkValueToProtoValue("notInt", intType, "colInt")
    }.getMessage should include("Cannot convert notInt to INT")
  }

  // --------------------------------------------------------------------------
  // 5) toComparableSparkValue (protoValueToSparkValue)
  // --------------------------------------------------------------------------
  behavior of "SparkToDASConverter.toComparableSparkValue"

  it should "convert DAS int value to Spark int" in {
    val dasInt = dasTypes.Value
      .newBuilder()
      .setInt(dasTypes.ValueInt.newBuilder().setV(100))
      .build()
    val sparkVal = SparkToDASConverter.toComparableSparkValue(dasInt, sparkTypes.IntegerType)
    sparkVal.get shouldBe 100
  }

  it should "convert DAS boolean value to Spark boolean" in {
    val dasBool = dasTypes.Value
      .newBuilder()
      .setBool(dasTypes.ValueBool.newBuilder().setV(true))
      .build()
    val sparkVal = SparkToDASConverter.toComparableSparkValue(dasBool, sparkTypes.BooleanType)
    sparkVal.get shouldBe true
  }

  it should "convert DAS string value to Spark string" in {
    val dasStr = dasTypes.Value
      .newBuilder()
      .setString(dasTypes.ValueString.newBuilder().setV("test"))
      .build()
    val sparkVal = SparkToDASConverter.toComparableSparkValue(dasStr, sparkTypes.StringType)
    sparkVal.get shouldBe "test"
  }

  it should "convert DAS date value to Spark Date" in {
    val dasDate = dasTypes.Value
      .newBuilder()
      .setDate(dasTypes.ValueDate.newBuilder().setYear(2025).setMonth(5).setDay(15))
      .build()
    val sparkVal = SparkToDASConverter.toComparableSparkValue(dasDate, sparkTypes.DateType)
    val expectedDate = Date.valueOf(LocalDate.of(2025, 5, 15))
    sparkVal.get shouldBe expectedDate
  }

  it should "convert DAS time value to Spark Time" in {
    val dasTime = dasTypes.Value
      .newBuilder()
      .setTime(dasTypes.ValueTime.newBuilder().setHour(12).setMinute(30).setSecond(45))
      .build()
    // Conversion logic for TIME does not depend on the provided Spark type
    val sparkVal = SparkToDASConverter.toComparableSparkValue(dasTime, sparkTypes.StringType)
    val expectedTime = Time.valueOf(LocalTime.of(12, 30, 45))
    sparkVal.get shouldBe expectedTime
  }

  it should "convert DAS timestamp value to Spark Timestamp" in {
    val dasTimestamp = dasTypes.Value
      .newBuilder()
      .setTimestamp(
        dasTypes.ValueTimestamp
          .newBuilder()
          .setYear(2025)
          .setMonth(4)
          .setDay(20)
          .setHour(10)
          .setMinute(20)
          .setSecond(30)
          .setNano(123456789))
      .build()
    val sparkVal = SparkToDASConverter.toComparableSparkValue(dasTimestamp, sparkTypes.TimestampType)
    val ldt = LocalDateTime.of(2025, 4, 20, 10, 20, 30, 123456789)
    val expectedTimestamp = Timestamp.valueOf(ldt)
    sparkVal.get shouldBe expectedTimestamp
  }

  it should "convert DAS interval value to CalendarInterval" in {
    val dasInterval = dasTypes.Value
      .newBuilder()
      .setInterval(
        dasTypes.ValueInterval
          .newBuilder()
          .setYears(2)
          .setMonths(3)
          .setDays(10)
          .setHours(5)
          .setMinutes(30)
          .setSeconds(20)
          .setMicros(500000))
      .build()
    val sparkVal = SparkToDASConverter.toComparableSparkValue(dasInterval, sparkTypes.CalendarIntervalType)
    val interval = sparkVal.get.asInstanceOf[CalendarInterval]
    val expectedMonths = 2 * 12 + 3 // 27
    val expectedDays = 10
    val expectedMicros = 5L * 3600000000L + 30L * 60000000L + 20L * 1000000L + 500000L
    interval.months shouldBe expectedMonths
    interval.days shouldBe expectedDays
    interval.microseconds shouldBe expectedMicros
  }

  it should "convert DAS record value to Map" in {
    val rec = dasTypes.ValueRecord
      .newBuilder()
      .addAtts(
        dasTypes.ValueRecordAttr
          .newBuilder()
          .setName("a")
          .setValue(dasTypes.Value.newBuilder().setInt(dasTypes.ValueInt.newBuilder().setV(5))))
      .addAtts(
        dasTypes.ValueRecordAttr
          .newBuilder()
          .setName("b")
          .setValue(dasTypes.Value.newBuilder().setString(dasTypes.ValueString.newBuilder().setV("test"))))
      .build()
    val dasRecVal = dasTypes.Value.newBuilder().setRecord(rec).build()
    // For record conversion, supply a corresponding Spark StructType.
    val structType = sparkTypes.StructType(
      Seq(
        sparkTypes.StructField("a", sparkTypes.IntegerType, nullable = true),
        sparkTypes.StructField("b", sparkTypes.StringType, nullable = true)))
    val sparkVal = SparkToDASConverter.toComparableSparkValue(dasRecVal, structType)
    sparkVal.get shouldBe Map("a" -> 5, "b" -> "test")
  }

  it should "convert DAS list value to Seq" in {
    val listBuilder = dasTypes.ValueList.newBuilder()
    listBuilder.addValues(dasTypes.Value.newBuilder().setDouble(dasTypes.ValueDouble.newBuilder().setV(1.1)))
    listBuilder.addValues(dasTypes.Value.newBuilder().setDouble(dasTypes.ValueDouble.newBuilder().setV(2.2)))
    val dasListVal = dasTypes.Value.newBuilder().setList(listBuilder.build()).build()
    // For list conversion, provide an ArrayType with element type DoubleType.
    val sparkVal = SparkToDASConverter.toComparableSparkValue(
      dasListVal,
      sparkTypes.ArrayType(sparkTypes.DoubleType, containsNull = true))
    sparkVal.get shouldBe Seq(1.1, 2.2)
  }

  it should "treat VarcharType as string" in {
    // e.g. Spark's VarcharType(100)
    val varcharSparkType = sparkTypes.VarcharType(100)
    val dasVarcharType = SparkToDASConverter.sparkTypeToDAS(varcharSparkType, nullable = true)

    dasVarcharType.hasString shouldBe true
    dasVarcharType.getString.getNullable shouldBe true
  }

  it should "convert DayTimeIntervalType to a DAS interval type" in {
    // Spark 3.x: DayTimeIntervalType()
    val dtInterval = sparkTypes.DayTimeIntervalType()
    val dasIntervalType = SparkToDASConverter.sparkTypeToDAS(dtInterval, nullable = false)

    dasIntervalType.hasInterval shouldBe true
    dasIntervalType.getInterval.getNullable shouldBe false
  }

  it should "convert YearMonthIntervalType to a DAS interval type" in {
    // Spark 3.x: YearMonthIntervalType()
    val ymInterval = sparkTypes.YearMonthIntervalType()
    val dasIntervalType = SparkToDASConverter.sparkTypeToDAS(ymInterval, nullable = true)

    dasIntervalType.hasInterval shouldBe true
    dasIntervalType.getInterval.getNullable shouldBe true
  }

}
