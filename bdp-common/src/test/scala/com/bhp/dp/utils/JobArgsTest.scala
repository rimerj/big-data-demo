package com.bhp.dp.utils

import com.bhp.dp.testutils.TestSparkSession
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JobArgsTest extends AnyFlatSpec with TestSparkSession {
  lazy val testSparkSession: SparkSession = sparkSession

  private val fakeArgs = Array(
    "-option_value", "optionally-awesome",
    "-test", "testvalue",
    "--dub_hyphen", "dubvalue",
    "-bool_test_pos", "1",
    "-bool_test_neg", "0",
    "-file_path_no_slash", "/mnt/test/path",
    "-file_path_slash", "/mnt/test/path/"
  )

  private val testSubject = new TestSubjectJobArgs(fakeArgs)

  behavior of "JobArgs.getArgStringValue"
  it should "parse the right value given input key" in {
    assert(testSubject.SingleHyphen == "testvalue", "just plain didn't parse right")
    assert(testSubject.DubHyphen == "dubvalue", "just plain didn't parse right")
    assert(testSubject.DefaultValueTest == "ASDFASDFASDFASDF", "Should have been the default value")
  }

  behavior of "JobArgs.getBooleanValue"
  it should "should parse out a true/false based on 1/0 in arg value" in {
    assert(testSubject.BoolTestPos, "didn't properly parse a boolean from a 1")
    assert(!testSubject.BoolTestNeg)
  }

  behavior of "JobArgs.ensureTrailingSlash"
  it should "ensure a tailing slash on all paths passed thru it" in {
    assert(testSubject.FilePathTestWithSlash == "/mnt/test/path/", "just plain didn't parse right")
    assert(testSubject.FilePathTestNoSlash == "/mnt/test/path/", "expecting a trailing slash")
  }

  behavior of "JobArgs.getOptionalStringValue"
  it should "return None when option is not available" in {
    assert(testSubject.MissingOptionalStringValueTest.isEmpty, "just plain didn't parse right")
    assert(testSubject.PresentOptionalStringValueTest.contains("optionally-awesome"), "should have parsed to optionally present string")
  }
}

class TestSubjectJobArgs(args: Array[String]) extends JobArgs(args) {
  val SingleHyphen: String = getArgStringValue("-test", "testdefault")
  val DubHyphen: String = getArgStringValue("--dub_hyphen", "defaultdub")
  val BoolTestPos: Boolean = getArgBoolValue("-bool_test_pos", true )
  val BoolTestNeg: Boolean = getArgBoolValue("-bool_test_neg", false)
  val FilePathTestWithSlash: String = ensureTrailingSlash(getArgStringValue("-file_path_slash", "/test/path"))
  val FilePathTestNoSlash: String = ensureTrailingSlash(getArgStringValue("-file_path_no_slash", "/test/path"))
  val DefaultValueTest: String = getArgStringValue("-non_existent", "ASDFASDFASDFASDF")
  val MissingOptionalStringValueTest: Option[String] = getOptionalStringValue("-non_existent")
  val PresentOptionalStringValueTest: Option[String] = getOptionalStringValue("-option_value")
}