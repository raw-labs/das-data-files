package com.rawlabs.das.datafile

import com.rawlabs.das.sdk.DASSdkException
import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.protocol.das.v1.query.{Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.{ColumnDefinition, TableDefinition, TableId, Row => ProtoRow}
import com.rawlabs.protocol.das.v1.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonTable(tableName: String, url: String, options: Map[String, String], spark: SparkSession)
  extends BaseDataFileTable(tableName, url) {

  override def format: String = "json"

  /**
   * Build the table definition for the JSON file.
   */
  override val tableDefinition: TableDefinition = {
    val builder = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName(tableName))
      .setDescription(s"JSON Table reading from $url")

    columns.foreach { case (colName, colType) =>
      builder.addColumns(ColumnDefinition.newBuilder().setName(colName).setType(colType))
    }

    builder.build()
  }

  // --------------------------------------------------------------------------
  // 2) Implement standard DASTable methods
  // --------------------------------------------------------------------------


  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    // We can't easily know row counts without reading the file.
    // Here, just guess or do some sampling logic if you wish:
    DASTable.TableEstimate(expectedNumberOfRows = 10000, avgRowWidthBytes = 100)
  }

  /**
   * Override to read JSON with Spark, parse any relevant options from the `options` map.
   */
  override protected def loadDataFrame(): DataFrame = {
    // For example, we might want multiLine option if reading multi-line JSON:
    val multiLine = options.get("multiLine").exists(_.toBoolean)

    spark.read
      .option("multiLine", multiLine.toString)
      .option("inferSchema", "true")
      .json(url)
  }
}
