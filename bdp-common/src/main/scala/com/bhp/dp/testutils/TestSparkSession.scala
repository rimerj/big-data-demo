package com.bhp.dp.testutils

import org.apache.spark.sql.SparkSession

trait TestSparkSession {
  private var sparkSessionPerTrait: SparkSession = null

  // this ensures log suppression is called per Test class, even if the singleton spark session is initialized
  implicit def sparkSession: SparkSession = this.synchronized {
    if (sparkSessionPerTrait == null) {
      TestSparkSessionEx.suppressSparkLogging()
      sparkSessionPerTrait = TestSparkSessionEx.sparkSession
    }
    sparkSessionPerTrait
  }
}
