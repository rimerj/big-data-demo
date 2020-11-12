package com.bhp.dp.demo

import com.bhp.dp.utils.JobArgs

final class DemoLoaderArgs(args: Array[String], isLocal: Boolean = false) extends JobArgs(args) {
  val LocalRun: Boolean = isLocal
  val TargetPath: String = getArgStringValue("-target", "./test_output")
}