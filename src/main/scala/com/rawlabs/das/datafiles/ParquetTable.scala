package com.rawlabs.das.datafiles

import com.rawlabs.das.datafiles.BaseDataFileTable
import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.protocol.das.v1.query.Qual
import com.rawlabs.protocol.das.v1.tables.{ColumnDefinition, TableDefinition, TableId}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Table that reads a Parquet file.
 */
class ParquetTable(tableName: String, url: String, options: Map[String, String], spark: SparkSession)
  extends BaseDataFileTable(tableName) {

  override def format: String = "parquet"

  /**
   * Build the table definition for the Parquet file.
   */
  override val tableDefinition: TableDefinition = {
    val builder = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName(tableName))
      .setDescription(s"Parquet Table reading from $url")

    // The 'columns' come from the parent class: we lazily load the DataFrame and infer schema.
    columns.foreach { case (colName, colType) =>
      builder.addColumns(
        ColumnDefinition.newBuilder().setName(colName).setType(colType)
      )
    }

    builder.build()
  }

  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    // Parquet has metadata that might let you guess row count or compression ratio,
    // but here we just do a rough guess:
    DASTable.TableEstimate(expectedNumberOfRows = 10000, avgRowWidthBytes = 200)
  }

  /**
   * Override to read Parquet with Spark.
   * Typically, we do not need 'inferSchema' for Parquet because it is stored in the file.
   */
  override protected def loadDataFrame(): DataFrame = {
    // If the user provided additional Spark options for Parquet, parse them here.
    // For example, "mergeSchema", "datetimeRebaseMode", etc.
    val reader = spark.read.format("parquet")
    options.foreach { case (key, value) =>
      reader.option(key, value)
    }
    reader.load(url)
  }

}