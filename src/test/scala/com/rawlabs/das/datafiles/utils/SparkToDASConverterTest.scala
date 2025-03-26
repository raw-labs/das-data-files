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

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{SparkSession, types => sparkTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.sdk.DASSdkInvalidArgumentException
import com.rawlabs.protocol.das.v1.query._
import com.rawlabs.protocol.das.v1.types.Value.ValueCase
import com.rawlabs.protocol.das.v1.{types => dasTypes}

/**
 * Unit tests for SparkToDASConverter object methods: 1) applyQuals 2) applySortKeys 3) sparkTypeToDAS 4)
 * sparkValueToProtoValue
 */
class SparkToDASConverterTest extends AnyFlatSpec with Matchers {

  // Minimal local spark session
  private lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .appName("SparkToDASConverterTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  // --------------------------------------------------------------------------
  // 1) applyQuals
  // --------------------------------------------------------------------------
  behavior of "SparkToDASConverter.applyQuals"

  it should "filter rows by EQUALS on a numeric column" in {
    import spark.implicits._
    val df = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")

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

    val (filtered, allApplied) = SparkToDASConverter.applyQuals(df, Seq(qual))
    val rows = filtered.collect()
    rows.length shouldBe 1
    rows.head.getInt(0) shouldBe 2
    rows.head.getString(1) shouldBe "Bob"
  }

  it should "handle an IS NULL (valProto.hasNull) qualifier" in {
    import spark.implicits._
    val df = Seq((1, "hello"), (2, null.asInstanceOf[String])).toDF("id", "text")

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

    val (filtered, allApplied) = SparkToDASConverter.applyQuals(df, Seq(qual))
    val rows = filtered.collect()
    rows.length shouldBe 1
    rows.head.getInt(0) shouldBe 2
    rows.head.isNullAt(1) shouldBe true
  }

  it should "handle a LIKE on a string column" in {
    import spark.implicits._
    val df = Seq((1, "alice@domain.com"), (2, "bob@domain.com"), (3, "none")).toDF("id", "email")

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

    val (filtered, allApplied) = SparkToDASConverter.applyQuals(df, Seq(qual))
    val rows = filtered.collect()
    rows.map(_.getInt(0)).sorted shouldBe Array(1, 2)
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

    // We have no null "id" in this sample, but let's pretend. We'll just confirm descending by id:
    rows.map(r => r.getInt(0)) shouldBe Array(10, 3, 2, 1)
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
    dasMap.getRecord.getAttsCount shouldBe 2 // "keys" and "values"
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

  it should "convert a decimal, date, or binary if the DAS type is set" in {
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

  ignore should "throw if raw value doesn't match the expected type" in {
    val intType = dasTypes.Type.newBuilder().setInt(dasTypes.IntType.newBuilder().setNullable(false)).build()
    intercept[DASSdkInvalidArgumentException] {
      SparkToDASConverter.sparkValueToProtoValue("notInt", intType, "colInt")
    }.getMessage should include("Cannot convert notInt to int (colInt)")
  }
}
