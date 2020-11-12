package com.bhp.dp.demo

import com.bhp.dp.demo.models.static_input_model
import com.bhp.dp.testutils.{TestHelpers, TestSparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.scalatest.flatspec.AnyFlatSpec

class DemoLoaderTest extends AnyFlatSpec with TestSparkSession with TestHelpers {
  private val testSparkSession = sparkSession
  behavior of "DemoLoader.etl"
  it should "produce expected results given input" in {
    import testSparkSession.implicits._

    //prep
    val dummyDemo = psvStringToDf(testSparkSession,
      """npi|tin|sk_loomis_provider_key
        |1234|1234|1
        |1235|1235|2
        |1236|1236|3
        |""".stripMargin)

    val dummyStatic = Seq(
      static_input_model(string_field = "1234", integer_field = 9, date_field = java.sql.Date.valueOf("2020-01-01")),
      static_input_model(string_field = "1235", integer_field = 8, date_field = java.sql.Date.valueOf("2020-02-04")),
      static_input_model(string_field = "1236", integer_field = 7, date_field = java.sql.Date.valueOf("2020-03-05")),
      static_input_model(string_field = "1236", integer_field = 8, date_field = java.sql.Date.valueOf("2020-03-06"))
    ).toDS

    val expected = psvStringToDf(testSparkSession,
      """pk_col|static_int_field|a_date|tin_order_by_npi
        |1234|9|2020-01-01|1
        |1235|8|2020-02-04|1
        |1236|7|2020-03-05|2
        |1236|8|2020-03-06|1
        |""".stripMargin)
      .withColumn("static_int_field", $"static_int_field".cast(IntegerType))
      .withColumn("a_date", $"a_date".cast(DateType))
      .withColumn("tin_order_by_npi", $"tin_order_by_npi".cast(IntegerType))

    //act
    val result = DemoLoader.etl(testSparkSession, dummyDemo, dummyStatic)

    //assert
    compareDataFrames(result, expected, Seq("pk_col", "tin_order_by_npi"), SparseDiffResults)
  }
}
