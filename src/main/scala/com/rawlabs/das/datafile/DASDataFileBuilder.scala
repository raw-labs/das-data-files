package com.rawlabs.das.datafile

import com.rawlabs.das.sdk.DASSettings
import com.rawlabs.das.sdk.scala.{DASSdk, DASSdkBuilder}

/**
 * Builder for the "http" DAS type. The engine calls build() with the user-provided config, returning a new DASHttp
 * instance.
 */
class DASDataFileBuilder extends DASSdkBuilder {

  // This must match your "type" field in the config for the plugin
  override def dasType: String = "datafiles"

  override def build(options: Map[String, String])(implicit settings: DASSettings): DASSdk = {
    new DASDataFile(options)
  }
}
