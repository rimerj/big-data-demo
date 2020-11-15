package com.bhp.dp.demo
import java.io._

import com.bhp.dp.demo.models.static_input_model
import com.bhp.dp.demo.sources.DemoSources
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}


object DemoLoader extends Serializable {
  final def main(args: Array[String]): Unit = {
    // CONFIGURATION
    val runConfig = new DemoLoaderArgs(args, localRun)
    val writeDestination: String = s"${runConfig.TargetPath}demo_loader_output.parquet"

    // SPARK SESSION
    val spark = establishSparkSesh

    // LOAD INPUTS
    val demoDf = DemoSources.loadDemoInput(spark, runConfig.LocalRun)
    val staticDf = DemoSources.loadStaticInput(spark)

    // SPECIFY OUTPUT
    val resultDf = etl(spark, demoDf, staticDf)

    // GENERATE OUTPUT FROM SPECIFICATION
    writeParquet(resultDf, writeDestination)
  }

  def etl(sparkSession: SparkSession, demo: DataFrame, staticDf: Dataset[static_input_model]): DataFrame = {
    import sparkSession.implicits._

    demo.as("d")
      .where($"tin" > lit(0))
      .join(staticDf.as("s"), $"d.npi" === $"s.string_field", "inner")
      .select(
        $"s.string_field".as("pk_col"),
        $"s.integer_field".as("static_int_field"),
        $"s.date_field".as("a_date"),
        row_number.over(Window.partitionBy($"d.npi").orderBy($"d.tin")).as("tin_order_by_npi")
      )
  }

  private def establishSparkSesh: SparkSession = SparkSession.builder
    .appName("DEMO").enableHiveSupport()
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.driver.maxResultSize", "40g")
    .config("spark.rpc.message.maxSize", "2047") // in MB - max in spark 3.0.1 is 2047 :(
    .getOrCreate

  private def localRun: Boolean = {
    val osString: String = System.getProperty("os.name").toUpperCase;
    osString.contains("WINDOWS") || osString.contains("MACOS")
  }

  def writeParquet(df: DataFrame, location: String): Unit = {
    println(s" === Writing $location")
    df.write.mode(SaveMode.Overwrite).parquet(location)
  }
}