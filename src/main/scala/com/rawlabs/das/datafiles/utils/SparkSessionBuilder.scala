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

package com.rawlabs.das.datafiles.utils

import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.apache.spark.sql.SparkSession

import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSettings}
import com.typesafe.config.ConfigFactory

object SparkSessionBuilder {

  def build(appName: String, options: Map[String, String])(implicit settings: DASSettings): SparkSession = {

    val builder = SparkSession
      .builder()
      .appName(appName)

    val conf = settings.getConfigSubTree("das.data-files").get.asScala.head.getValue.atKey("spark-config")

    val sparkConfig = conf
      .entrySet()
      .asScala
      .map { entry =>
        val key = entry.getKey
        key.stripPrefix("spark-config.") -> conf.getString(key)
      }
      .toMap

    builder.config(sparkConfig)

    // Creating a new session and applying the configuration for this das (s3 config for now)
    val newSession = builder
      .getOrCreate()
      .newSession()

    newSession
  }
}
