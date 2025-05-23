import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.*
import de.heikoseeberger.sbtheader.{CommentCreator, HeaderPlugin}
import sbt.*
import sbt.Keys.*

object CopyrightHeader extends AutoPlugin {

  override def requires: Plugins = HeaderPlugin

  override def trigger: PluginTrigger = allRequirements

  protected def headerMappingSettings: Seq[Def.Setting[_]] = Seq(Compile, Test).flatMap { config =>
    inConfig(config)(
      Seq(
        headerLicense := Some(HeaderLicense.Custom(header)),
        headerMappings := headerMappings.value ++ Map(
          HeaderFileType.scala -> cStyleComment,
          HeaderFileType.java -> cStyleComment
        )
      )
    )
  }

  override def projectSettings: Seq[Def.Setting[_]] = Def.settings(headerMappingSettings, additional)

  def additional: Seq[Def.Setting[_]] = Def.settings(
    Compile / compile := {
      (Compile / headerCreate).value
      (Compile / compile).value
    },
    Test / compile := {
      (Test / headerCreate).value
      (Test / compile).value
    }
  )

  def header: String = {
    val currentYear = "2025"
    s"""|/*
        | * Copyright $currentYear RAW Labs S.A.
        | *
        | * Use of this software is governed by the Business Source License
        | * included in the file licenses/BSL.txt.
        | *
        | * As of the Change Date specified in that file, in accordance with
        | * the Business Source License, use of this software will be governed
        | * by the Apache License, Version 2.0, included in the file
        | * licenses/APL.txt.
        | */""".stripMargin
  }

  val cStyleComment = HeaderCommentStyle.cStyleBlockComment.copy(commentCreator = new CommentCreator() {
    val CopyrightPattern = "Copyright (\\d{4}) RAW Labs S.A.".r

    override def apply(text: String, existingText: Option[String]): String = {
      existingText match {
        case Some(existingHeader) if CopyrightPattern.findFirstIn(existingHeader).isDefined =>
          // matches the pattern with any year, return it unchanged
          existingHeader.trim
        case _ => header
      }
    }
  })

}
