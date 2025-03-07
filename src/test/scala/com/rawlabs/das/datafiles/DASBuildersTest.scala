package com.rawlabs.das.datafiles

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.datafiles.table._
import com.rawlabs.das.sdk.DASSdkInvalidArgumentException
import com.rawlabs.das.sdk.DASSettings

class DASBuildersTest extends AnyFlatSpec with Matchers {

  // Provide a minimal implicit DASSettings for the build() calls
  implicit val settings: DASSettings = new DASSettings {}

  behavior of "DASHttpCsvBuilder"

  it should "build a DASHttpCsv with an http CSV table if url starts with http" in {
    val options = Map(
      "nr_tables" -> "1",
      "table0_url" -> "http://example.com/data.csv",
      "table0_format" -> "csv"
      // plus any other required config keys...
    )

    val sdk = new DASHttpCsvBuilder().build(options)
    sdk shouldBe a[DASHttpCsv]

    val httpCsv = sdk.asInstanceOf[DASHttpCsv]
    httpCsv.tables.size shouldBe 1
    val (_, table) = httpCsv.tables.head
    table shouldBe a[CsvTable]
  }

  it should "throw an exception if url does not start with http or https" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "s3://mybucket/data.csv", "table0_format" -> "csv")

    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASHttpCsvBuilder().build(options)
    }
  }

  // ------------------------------------------------
  behavior of "DASHttpJsonBuilder"

  it should "build a DASHttpJson with an http JSON table if url starts with http" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "https://example.com/data.json", "table0_format" -> "json")

    val sdk = new DASHttpJsonBuilder().build(options)
    sdk shouldBe a[DASHttpJson]

    val httpJson = sdk.asInstanceOf[DASHttpJson]
    httpJson.tables.size shouldBe 1
    val (_, table) = httpJson.tables.head
    table shouldBe a[JsonTable]
  }

  it should "throw an exception if url does not start with http or https for JSON" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "file:///local/path.json", "table0_format" -> "json")
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASHttpJsonBuilder().build(options)
    }
  }

  // ------------------------------------------------
  behavior of "DASHttpParquetBuilder"

  it should "build a DASHttpParquet with an http Parquet table if url starts with http" in {
    val options =
      Map("nr_tables" -> "1", "table0_url" -> "http://example.com/data.parquet", "table0_format" -> "parquet")

    val sdk = new DASHttpParquetBuilder().build(options)
    sdk shouldBe a[DASHttpParquet]

    val httpParquet = sdk.asInstanceOf[DASHttpParquet]
    httpParquet.tables.size shouldBe 1
    val (_, table) = httpParquet.tables.head
    table shouldBe a[ParquetTable]
  }

  it should "throw an exception if url does not start with http or https for Parquet" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "s3://bucket/file.parquet", "table0_format" -> "parquet")
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASHttpParquetBuilder().build(options)
    }
  }

  // ------------------------------------------------
  behavior of "DASS3CsvBuilder"

  it should "build a DASS3Csv with an s3 CSV table if url starts with s3" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "s3://mybucket/data.csv", "table0_format" -> "csv")

    val sdk = new DASS3CsvBuilder().build(options)
    sdk shouldBe a[DASS3Csv]

    val s3Csv = sdk.asInstanceOf[DASS3Csv]
    s3Csv.tables.size shouldBe 1
    val (_, table) = s3Csv.tables.head
    table shouldBe a[CsvTable]
  }

  it should "throw an exception if url does not start with s3 for CSV" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "https://example.com/data.csv", "table0_format" -> "csv")
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASS3CsvBuilder().build(options)
    }
  }

  // ------------------------------------------------
  behavior of "DASS3JsonBuilder"

  it should "build a DASS3Json with an s3 JSON table if url starts with s3" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "s3://some-bucket/data.json", "table0_format" -> "json")

    val sdk = new DASS3JsonBuilder().build(options)
    sdk shouldBe a[DASS3Json]

    val s3Json = sdk.asInstanceOf[DASS3Json]
    s3Json.tables.size shouldBe 1
    val (_, table) = s3Json.tables.head
    table shouldBe a[JsonTable]
  }

  it should "throw an exception if url does not start with s3 for JSON" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "http://example.com/data.json", "table0_format" -> "json")
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASS3JsonBuilder().build(options)
    }
  }

  // ------------------------------------------------
  behavior of "DASS3ParquetBuilder"

  it should "build a DASS3Parquet with an s3 Parquet table if url starts with s3" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "s3://some-bucket/data.parquet", "table0_format" -> "parquet")

    val sdk = new DASS3ParquetBuilder().build(options)
    sdk shouldBe a[DASS3Parquet]

    val s3Parquet = sdk.asInstanceOf[DASS3Parquet]
    s3Parquet.tables.size shouldBe 1
    val (_, table) = s3Parquet.tables.head
    table shouldBe a[ParquetTable]
  }

  it should "throw an exception if url does not start with s3 for Parquet" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "https://host/data.parquet", "table0_format" -> "parquet")
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASS3ParquetBuilder().build(options)
    }
  }

  // ------------------------------------------------
  behavior of "DASS3XmlBuilder"

  it should "build a DASS3Xml with an s3 XML table if url starts with s3" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "s3://some-bucket/data.xml", "table0_format" -> "xml")

    val sdk = new DASS3XmlBuilder().build(options)
    sdk shouldBe a[DASS3Xml]

    val s3Xml = sdk.asInstanceOf[DASS3Xml]
    s3Xml.tables.size shouldBe 1
    val (_, table) = s3Xml.tables.head
    table shouldBe a[XmlTable]
  }

  it should "throw an exception if url does not start with s3 for XML" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "http://example.com/data.xml", "table0_format" -> "xml")
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASS3XmlBuilder().build(options)
    }
  }

  // ------------------------------------------------
  behavior of "DASHttpXmlBuilder"

  it should "build a DASHttpXml with an http/https XML table if url starts with http/https" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "https://host/data.xml", "table0_format" -> "xml")

    val sdk = new DASHttpXmlBuilder().build(options)
    sdk shouldBe a[DASHttpXml]

    val httpXml = sdk.asInstanceOf[DASHttpXml]
    httpXml.tables.size shouldBe 1
    val (_, table) = httpXml.tables.head
    table shouldBe a[XmlTable]
  }

  it should "throw an exception if url is not http/https for DASHttpXml" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "s3://some-bucket/data.xml", "table0_format" -> "xml")
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASHttpXmlBuilder().build(options)
    }
  }

  // ------------------------------------------------
  behavior of "DASDataFilesBuilder"

  it should "build a DASDataFiles that supports various formats from any local or non-specific URL" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "/local/path/file.csv", "table0_format" -> "csv")
    val sdk = new DASDataFilesBuilder().build(options)
    sdk shouldBe a[DASDataFiles]

    val dataFiles = sdk.asInstanceOf[DASDataFiles]
    dataFiles.tables.size shouldBe 1
    val (_, table) = dataFiles.tables.head
    table shouldBe a[CsvTable]
  }

  it should "throw an exception if format is missing for DASDataFiles" in {
    val options = Map(
      "nr_tables" -> "1",
      "table0_url" -> "/local/path/file.csv"
      // missing table0_format
    )
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASDataFilesBuilder().build(options)
    }
  }
}
