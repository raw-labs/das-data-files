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

package com.rawlabs.das.datafiles.filesystem

sealed trait FileSystemError

object FileSystemError {
  final case class NotFound(url: String) extends FileSystemError
  final case class InvalidUrl(url: String, message: String) extends FileSystemError
  final case class PermissionDenied(message: String) extends FileSystemError
  final case class Unauthorized(message: String) extends FileSystemError
  final case class Unsupported(message: String) extends FileSystemError
  final case class TooManyRequests(message: String) extends FileSystemError
  final case class FileTooLarge(url: String, size: Long, max: Long) extends FileSystemError
}
