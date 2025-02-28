package com.rawlabs.das.datafile

import com.rawlabs.das.sdk.DASSdkException
import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.protocol.das.v1.query.{Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.{ColumnDefinition, TableDefinition, TableId, Row => ProtoRow}
import com.rawlabs.protocol.das.v1.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

class CsvTable(tableName: String, url: String, options: Map[String, String], spark: SparkSession)
    extends BaseDataFileTable(tableName, url) {

  override def format: String = "csv"


  /**
   * Build the table definition for the CSV.
   */
  override val tableDefinition: TableDefinition = {
    val builder = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName(tableName))
      .setDescription(s"CSV Table reading from $url")

    columns.foreach { case (colName, colType) =>
      builder.addColumns(ColumnDefinition.newBuilder().setName(colName).setType(colType))
    }

    builder.build()
  }

  // --------------------------------------------------------------------------
  // 2) Implement standard DASTable methods
  // --------------------------------------------------------------------------
  override def getTablePathKeys: Seq[com.rawlabs.protocol.das.v1.query.PathKey] = Seq.empty
  override def getTableSortOrders(sortKeys: Seq[SortKey]): Seq[SortKey] = Seq.empty

  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    // We can't easily know row counts without reading the file, so just guess.
    // Or you could read the file once to see how many lines it has.
    DASTable.TableEstimate(expectedNumberOfRows = 10000, avgRowWidthBytes = 100)
  }

  /**
   * Override to read CSV with Spark, parse relevant CSV options from `options`.
   */
  override protected def loadDataFrame(): DataFrame = {
    // Parse CSV-specific options from the `options` map. For example:
    val hasHeader = options.get("header").forall(_.toBoolean)
    val delimiter = options.getOrElse("delimiter", ",")

    spark.read
      .option("header", hasHeader.toString)
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(url)
  }

  // Mark read-only
  override def insert(row: ProtoRow): ProtoRow =
    throw new DASSdkException(s"CSV table '$tableName' is read-only.")
  override def update(rowId: Value, newRow: ProtoRow): ProtoRow =
    throw new DASSdkException(s"CSV table '$tableName' is read-only.")
  override def delete(rowId: Value): Unit =
    throw new DASSdkException(s"CSV table '$tableName' is read-only.")


}
