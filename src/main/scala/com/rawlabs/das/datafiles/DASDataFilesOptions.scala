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

package com.rawlabs.das.datafiles

import scala.collection.mutable

import com.rawlabs.das.sdk.DASSdkInvalidArgumentException

/**
 * Represents a single table’s configuration
 */

case class HttpConnectionOptions(followRedirects: Boolean, connectTimeout: Int, sslTrustAll: Boolean)
case class awsCredential(accessKey: String, secretKey: String)

case class DataFileConfig(name: String, url: String, format: Option[String], options: Map[String, String])

/**
 * Holds all parsed config from user’s definition for the entire DAS.
 */
class DASDataFilesOptions(options: Map[String, String]) {

  // Number of tables to load, e.g. nr_tables=3 => table0_..., table1_..., table2_...
  val nrTables: Int = options.get("nr_tables").map(_.toInt).getOrElse(1)

  val s3Credentials: Option[awsCredential] = options.get("aws_access_key").map { accessKey =>
    val secretKey =
      options.getOrElse("aws_secret_key", throw new DASSdkInvalidArgumentException("aws_secret_key not found"))
    awsCredential(accessKey, secretKey)
  }

  val extraSparkConfig: Map[String, String] = options.filter(x => x._1.startsWith("extra_config_"))

  val httpOptions: HttpConnectionOptions = {
    val followRedirects = options.getOrElse("http_follow_redirects", "true").toBoolean
    val connectTimeout = options.getOrElse("http_connect_timeout", "10000").toInt
    val sslTRustAll = options.getOrElse("http_ssl_trust_all", "false").toBoolean
    HttpConnectionOptions(followRedirects, connectTimeout, sslTRustAll)
  }

  // Keep track of used names so we ensure uniqueness
  private val usedNames = mutable.Set[String]()

  /**
   * Build a list of DataFileConfig from user’s config keys.
   *   - table0_url, table0_format, table0_name (optional), ...
   *   - table1_url, table1_format, table1_name (optional), ...
   */
  val tableConfigs: Seq[DataFileConfig] = {
    (0 until nrTables).map { i =>
      val prefix = s"table${i}_"

      // Mandatory fields
      val url =
        options.getOrElse(
          prefix + "url",
          throw new DASSdkInvalidArgumentException(s"Missing '${prefix}url' option for DataFile DAS."))
      val format = options.get(prefix + "format")

      // Name is optional: if not provided, derive from URL
      val maybeName = options.get(prefix + "name")
      val tableName = maybeName match {
        case Some(n) => ensureUniqueName(n)
        case None    =>
          // Derive from file name in URL if not explicitly defined
          val derived = deriveNameFromUrl(url)
          ensureUniqueName(derived)
      }

      // Gather all prefixed options into a sub-map for this table
      val tableOptions = options.collect {
        case (k, v) if k.startsWith(prefix) => (k.drop(prefix.length), v)
      }.toMap

      DataFileConfig(name = tableName, url = url, format = format, options = tableOptions)
    }
  }

  /**
   * Given a URL, derive the table name from the filename. E.g. "https://host/path/data.csv" => "data_csv"
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
