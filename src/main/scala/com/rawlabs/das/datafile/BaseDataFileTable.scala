package com.rawlabs.das.datafile

import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.das.sdk.{DASExecuteResult, DASSdkException}
import com.rawlabs.protocol.das.v1.query.{Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.{TableDefinition, Column => ProtoColumn, Row => ProtoRow}
import com.rawlabs.protocol.das.v1.types._
import org.apache.spark.sql.{DataFrame, types => sparkTypes}

import scala.jdk.CollectionConverters._

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
          val dasType = colTypesMap.getOrElse(col, mkStringType())
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
  protected def applyQuals(df: DataFrame, quals: Seq[Qual]): DataFrame = {
    // For example, only handle EQUALS for string/int/bool
    import org.apache.spark.sql.functions._
    var result = df
    for (q <- quals) {
      if (q.hasSimpleQual) {
        val sq = q.getSimpleQual
        val op = sq.getOperator
        val colName = q.getName
        val valProto = sq.getValue

        // Only handle EQUALS in this base example
        if (op.getNumber != com.rawlabs.protocol.das.v1.query.Operator.EQUALS_VALUE) {
          // skip or throw
          throw new DASSdkException(s"Only EQUALS operator supported (col=$colName). Found: $op")
        }

        val filterCol = col(colName)
        if (valProto.hasString) {
          result = result.filter(filterCol === valProto.getString.getV)
        } else if (valProto.hasInt) {
          result = result.filter(filterCol === valProto.getInt.getV)
        } else if (valProto.hasBool) {
          result = result.filter(filterCol === valProto.getBool.getV)
        } else {
          throw new DASSdkException(s"Unsupported filter type on col=$colName in base class.")
        }
      }
    }
    result
  }

  // -------------------------------------------------------------------
  // 5) Utilities for schema mapping, building proto Values, etc.
  // -------------------------------------------------------------------
  protected def sparkTypeToDAS(dt: org.apache.spark.sql.types.DataType, nullable: Boolean): Type = {
    dt match {
      case sparkTypes.StringType  => Type.newBuilder().setString(StringType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.IntegerType => Type.newBuilder().setInt(IntType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.LongType    => Type.newBuilder().setLong(LongType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.DoubleType  => Type.newBuilder().setDouble(DoubleType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.FloatType   => Type.newBuilder().setFloat(FloatType.newBuilder().setNullable(nullable)).build()
      case sparkTypes.BooleanType => Type.newBuilder().setBool(BoolType.newBuilder().setNullable(nullable)).build()
      // fallback to string
      case _ => Type.newBuilder().setString(StringType.newBuilder().setNullable(nullable)).build()
    }
  }

  protected def rawToProtoValue(rawValue: Any, dasType: Type, colName: String): Value = {
    if (rawValue == null) {
      return Value
        .newBuilder()
        .setNull(ValueNull.newBuilder().build())
        .build()
    }

    if (dasType.hasString) {
      Value.newBuilder().setString(ValueString.newBuilder().setV(rawValue.toString)).build()
    } else if (dasType.hasInt) {
      val intVal = rawValue match {
        case i: Int    => i
        case l: Long   => l.toInt
        case s: String => s.toInt
        case _         => throw new DASSdkException(s"Cannot convert $rawValue to int ($colName)")
      }
      Value.newBuilder().setInt(ValueInt.newBuilder().setV(intVal)).build()
    } else if (dasType.hasLong) {
      val longVal = rawValue match {
        case l: Long   => l
        case i: Int    => i.toLong
        case s: String => s.toLong
        case _         => throw new DASSdkException(s"Cannot convert $rawValue to long ($colName)")
      }
      Value.newBuilder().setLong(ValueLong.newBuilder().setV(longVal)).build()
    } else if (dasType.hasBool) {
      val boolVal = rawValue match {
        case b: Boolean => b
        case s: String  => s.toBoolean
        case _          => throw new DASSdkException(s"Cannot convert $rawValue to bool ($colName)")
      }
      Value.newBuilder().setBool(ValueBool.newBuilder().setV(boolVal)).build()
    } else if (dasType.hasDouble) {
      val dblVal = rawValue match {
        case d: Double => d
        case f: Float  => f.toDouble
        case s: String => s.toDouble
        case _         => throw new DASSdkException(s"Cannot convert $rawValue to double ($colName)")
      }
      Value.newBuilder().setDouble(ValueDouble.newBuilder().setV(dblVal)).build()
    } else if (dasType.hasFloat) {
      val fltVal = rawValue match {
        case f: Float  => f
        case d: Double => d.toFloat
        case s: String => s.toFloat
        case _         => throw new DASSdkException(s"Cannot convert $rawValue to float ($colName)")
      }
      Value.newBuilder().setFloat(ValueFloat.newBuilder().setV(fltVal)).build()
    } else {
      // fallback to string
      Value.newBuilder().setString(ValueString.newBuilder().setV(rawValue.toString)).build()
    }
  }

  protected def mkStringType(): Type = {
    Type.newBuilder().setString(StringType.newBuilder().setNullable(true)).build()
  }
}
