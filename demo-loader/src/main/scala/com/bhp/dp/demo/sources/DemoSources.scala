package com.bhp.dp.demo.sources

import com.bhp.dp.demo.models.static_input_model
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DemoSources {
  def loadDemoInput(sparkSession: SparkSession, isLocal: Boolean): DataFrame = {
    val local = if (isLocal) "." else ""
    sparkSession.read.parquet(s"$local/mnt/ods2p/demo/demo_input.parquet")
  }

  def loadStaticInput(sparkSession: SparkSession): Dataset[static_input_model] = {
    import sparkSession.implicits._

    Seq(
      static_input_model("test", 1, java.sql.Date.valueOf("2020-11-12")),
      static_input_model("1003026055", 2, java.sql.Date.valueOf("2020-11-12")),
      static_input_model("1003101684", 3, java.sql.Date.valueOf("2020-11-12")),
      static_input_model("1003102740", 4, java.sql.Date.valueOf("2020-11-12")),
      static_input_model("1003102815", 5, java.sql.Date.valueOf("2020-11-12")),
      static_input_model("1003115684", 6, java.sql.Date.valueOf("2020-11-12")),
      static_input_model("1003130592", 7, java.sql.Date.valueOf("2020-11-12")),
      static_input_model("1003162256", 8, java.sql.Date.valueOf("2020-11-12")),
      static_input_model("1003914201", 9, java.sql.Date.valueOf("2020-11-12"))
    ).toDS
  }
}