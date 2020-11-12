package com.bhp.dp.utils

import com.bhp.dp.testutils.TestSparkSession
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JarResourcesTest extends AnyFlatSpec with TestSparkSession {
  lazy val testSparkSession: SparkSession = sparkSession

  behavior of "JarResources.loadRawResource"
  it should "load expected content from JAR resources" in {
    val outcome = JarResources.loadRawResource("JarResourcesFileTest.csv")

    assert(outcome.trim.contains("test,test2"), "CSV Header not detected in parsed content")
  }
  behavior of "JarResources.parallelizeResource"
  it should "produce a dataframe with as many rows as lines of content" in {
    val outcome = JarResources.parallelizeResource("JarResourcesFileTest.csv", testSparkSession)

    assert(outcome.count == 2, "parallelized raw file should have two rows (lines)")
  }
}
