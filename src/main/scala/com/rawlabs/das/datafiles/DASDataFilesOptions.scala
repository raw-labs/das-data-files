/*
 * Copyright 2024 RAW Labs S.A.
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

import com.rawlabs.das.sdk.DASSdkException

/**
 * Represents a single table’s configuration
 */

case class HttpConnectionOptions(followRedirects: Boolean, connectTimeout: Int, readTimeout: Int, sslTRustAll: Boolean)

case class HttpFileConfig(
    method: String,
    headers: Map[String, String],
    body: Option[String],
    httpConnectionOptions: HttpConnectionOptions)

case class DataFileConfig(
    name: String,
    url: String,
    format: Option[String],
    options: Map[String, String],
    maybeHttpConfig: Option[HttpFileConfig])
case class awsCredential(accessKey: String, secretKey: String)

/**
 * Holds all parsed config from user’s definition for the entire DAS.
 */
class DASDataFilesOptions(options: Map[String, String]) {

  // Number of tables to load, e.g. nr_tables=3 => table0_..., table1_..., table2_...
  val nrTables: Int = options.get("nr_tables").map(_.toInt).getOrElse(1)

  val s3Credentials: Option[awsCredential] = options.get("awsAccessKey").map { accessKey =>
    val secretKey = options.getOrElse("awsSecretKey", throw new DASSdkException("aswSecretKey not found"))
    awsCredential(accessKey, secretKey)
  }

  val extraSparkConfig: Map[String, String] = options.filter(x => x._1.startsWith("extra_config_"))

  val httpConnectionOptions: HttpConnectionOptions = {
    val followRedirects = options.getOrElse("http_option_followRedirects", "true").toBoolean
    val connectTimeout = options.getOrElse("http_option_connectTimeout", "10000").toInt
    val readTimeout = options.getOrElse("http_option_readTimeout", "10000").toInt
    val sslTRustAll = options.getOrElse("http_option_sslTrustAll", "false").toBoolean
    HttpConnectionOptions(followRedirects, connectTimeout, readTimeout, sslTRustAll)
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
        options.getOrElse(prefix + "url", throw new DASSdkException(s"Missing '${prefix}url' option for DataFile DAS."))
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

      val maybeHttpConfig: Option[HttpFileConfig] = if (url.startsWith("http://") | url.startsWith("https://")) {
        val method = options.getOrElse(prefix + "http_method", "GET")
        val headerPrefix = s"${prefix}http_header_"
        val headers = options.collect {
          case (k, v) if k.startsWith(headerPrefix) => (k.drop(headerPrefix.length), v)
        }
        val body = options.get(prefix + "http_body")
        Some(HttpFileConfig(method, headers, body, httpConnectionOptions))
      } else {
        None
      }

      // Gather all prefixed options into a sub-map for this table
      val option_prefix = s"${prefix}option_"
      val tableOptions = options.collect {
        case (k, v) if k.startsWith(option_prefix) => (k.drop(option_prefix.length), v)
      }.toMap

      DataFileConfig(
        name = tableName,
        url = url,
        format = format,
        options = tableOptions,
        maybeHttpConfig = maybeHttpConfig)
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
