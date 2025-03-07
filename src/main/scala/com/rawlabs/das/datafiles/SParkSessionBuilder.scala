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

import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.apache.spark.sql.SparkSession

import com.typesafe.config.{Config, ConfigFactory}

object SParkSessionBuilder {

  def build(appName: String, options: DASDataFilesOptions): SparkSession = {
    val builder = SparkSession
      .builder()
      .appName(appName)

    // Load the Spark configuration from the config file
    val sparkConfig = ConfigFactory.load().getConfig("raw.das.data-files.spark")
    sparkConfig.entrySet().asScala.foreach { entry =>
      val key = entry.getKey
      builder.config("spark." + key, sparkConfig.getString(key))
    }

    // Set the S3 credentials if provided or use anonymous credentials
    options.s3Credentials match {
      case Some(creds) =>
        builder.config("fs.s3a.access.key", creds.accessKey)
        builder.config("fs.s3a.secret.key", creds.secretKey)
      case None =>
        builder.config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    }

    // Add any extra Spark configuration from the user
    builder
      .config(options.extraSparkConfig)
    builder.getOrCreate()
  }
}
