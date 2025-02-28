package com.rawlabs.das.datafile

import com.rawlabs.das.sdk.DASSdkException
import scala.collection.mutable

/**
 * Represents a single table’s configuration
 */
case class DataFileConfig(name: String, url: String, format: String, options: Map[String, String])

/**
 * Holds all parsed config from user’s definition for the entire DAS.
 */
class DASDataFileOptions(options: Map[String, String]) {

  // Number of tables to load, e.g. nr_tables=3 => table0_..., table1_..., table2_...
  val nrTables: Int = options.get("nr_tables").map(_.toInt).getOrElse(1)

  // Keep track of used names so we ensure uniqueness
  private val usedNames = mutable.Set[String]()

  /**
   * Build a list of DataFileConfig from user’s config keys.
   * - table0_url, table0_format, table0_name (optional), ...
   * - table1_url, table1_format, table1_name (optional), ...
   */
  val tableConfigs: Seq[DataFileConfig] = {
    (0 until nrTables).map { i =>
      val prefix = s"table${i}_"

      // Mandatory fields
      val url = options.getOrElse(
        prefix + "url",
        throw new DASSdkException(s"Missing '${prefix}url' option for DataFile DAS.")
      )
      val format = options.getOrElse(
        prefix + "format",
        throw new DASSdkException(s"Missing '${prefix}format' option for DataFile DAS.")
      )

      // Name is optional: if not provided, derive from URL
      val maybeName = options.get(prefix + "name")
      val tableName = maybeName match {
        case Some(n) => ensureUniqueName(n)
        case None =>
          // Derive from file name in URL if not explicitly defined
          val derived = deriveNameFromUrl(url)
          ensureUniqueName(derived)
      }

      val option_prefix = s"${prefix}option_"
      // Gather all prefixed options into a sub-map for this table
      val tableOptions = options.collect {
        case (k, v) if k.startsWith(option_prefix) => (k.drop(option_prefix.length), v)
      }.toMap

      DataFileConfig(
        name = tableName,
        url = url,
        format = format,
        options = tableOptions
      )
    }
  }

  /**
   * Given a URL, derive the table name from the filename. 
   * E.g. "https://host/path/data.csv" => "data_csv"
   */
  private def deriveNameFromUrl(url: String): String = {
    // Extract last path segment
    val filePart = url.split("/").lastOption.getOrElse(url)
    // Replace '.' with '_'
    filePart.replace('.', '_')
  }

  /**
   * Ensure the proposed name is unique by appending _2, _3, etc. as needed.
   */
  private def ensureUniqueName(base: String): String = {
    var finalName = base
    var n = 2
    while (usedNames.contains(finalName)) {
      finalName = s"${base}_$n"
      n += 1
    }
    usedNames += finalName
    finalName
  }

}