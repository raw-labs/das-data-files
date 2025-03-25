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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class SparkSessionBuilderTest extends AnyFlatSpec with Matchers {

  it should "set anonymous credentials when aws_access_key is not provided" in {
    val sparkSess = SparkSessionBuilder.build("testApp", Map.empty)
    sparkSess.conf.get("fs.s3a.aws.credentials.provider") shouldBe
    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
  }

  it should "set fs.s3a.access.key when aws_access_key is provided" in {
    val sparkSess =
      SparkSessionBuilder.build("testApp", Map("aws_access_key" -> "TESTKEY", "aws_secret_key" -> "TESTSECRET"))
    sparkSess.conf.get("fs.s3a.access.key") shouldBe "TESTKEY"
    sparkSess.conf.get("fs.s3a.secret.key") shouldBe "TESTSECRET"
  }
}
