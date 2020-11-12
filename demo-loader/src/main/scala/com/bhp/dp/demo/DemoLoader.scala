package com.bhp.dp.demo
import java.io._

import com.bhp.dp.demo.models.static_input_model
import com.bhp.dp.demo.sources.DemoSources
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}


object DemoLoader extends Serializable {
  final def main(args: Array[String]): Unit = {
    val runConfig = new DemoLoaderArgs(args, localRun)
    val writeDestination: String = "/mnt/ods2p/demo/demo_output.parquet"
    val spark = establishSparkSesh

    // LOAD INPUTS
    val demoDf = DemoSources.loadDemoInput(spark)
    val staticDf = DemoSources.loadStaticInput(spark)

    // SPECIFY OUTPUT
    val resultDf = etl(spark, demoDf, staticDf)

    // GENERATE OUTPUT FROM SPECIFICATION
    writeParquet(resultDf, s"${runConfig.TargetPath}writeDestination")
  }


  def etl(sparkSession: SparkSession, demo: DataFrame, staticDf: Dataset[static_input_model]): DataFrame = {
    import sparkSession.implicits._

    demo.as("d")
      .join(staticDf.as("s"), $"d.npi" === $"s.string_field", "inner")
      .select(
        $"s.string_field".as("pk_col"),
        $"s.integer_field".as("static_int_field"),
        $"s.date_field".as("a_date")
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

  def writeParquet(df: DataFrame, location: String): Unit = df.write.mode(SaveMode.Overwrite).parquet(location)
}