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

package com.rawlabs.das.datafiles.utils

import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.apache.spark.sql.SparkSession

import com.rawlabs.das.datafiles.filesystem.AwsSecretCredential
import com.typesafe.config.ConfigFactory

object SParkSessionBuilder {

  def build(appName: String, options: DASDataFilesOptions): SparkSession = {
    val builder = SparkSession
      .builder()
      .appName(appName)

    // Set the S3 credentials if provided or use anonymous credentials
    options.fileSystemCredential match {
      case Some(AwsSecretCredential(region, accessKey, secretKey)) =>
        builder.config("fs.s3a.access.key", accessKey)
        builder.config("fs.s3a.secret.key", secretKey)
        builder.config("fs.s3a.endpoint", s"s3.$region.amazonaws.com")
      case _ =>
        builder.config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    }

    // Load the Spark configuration from the config file
    val sparkConfig = ConfigFactory.load().getConfig("raw.das.data-files.spark-config")
    sparkConfig.entrySet().asScala.foreach { entry =>
      val key = entry.getKey
      builder.config(key, sparkConfig.getString(key))
    }

    builder.getOrCreate()
  }
}
