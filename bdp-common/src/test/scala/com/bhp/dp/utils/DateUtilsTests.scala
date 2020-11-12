package com.bhp.dp.utils

import java.sql.Date
import java.text.SimpleDateFormat

import com.bhp.dp.testutils.{TestHelpers, TestSparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.TableDrivenPropertyChecks

class DateUtilsTests extends AnyFlatSpec with TestSparkSession with TableDrivenPropertyChecks with TestHelpers {
  lazy val testSparkSession: SparkSession = sparkSession

  import testSparkSession.implicits._

  val expectedOutputStringFormatter = new SimpleDateFormat("yyyyMMdd")

  val inputAndExpectedResult =
    Table(
      ("input_date", "expected_date", "date_object"),
      ("20000102", "20000102", new Date(100, 0, 2)),
      ("2000-01-01", null, null),
      ("20001301", null, null),
      ("20000132", null, null),
      ("20000132", null, null),
      ("20000231", null, null), // 31st Feb Invalid
      ("20000430", "20000430", new Date(100, 3, 30)),
      ("20000431", null, null),
      ("21000229", null, null), // 2100 is NOT a leap year
      ("21000228", "21000228", new Date(200, 1, 28)),
      ("19000229", null, null), // 1900 was NOT a leap year
      ("19000228", "19000228", new Date(0, 1, 28)),

      ("20010132", null, null),
      ("20010131", "20010131", new Date(101, 0, 31)),

      ("20010229", null, null),
      ("20010228", "20010228", new Date(101, 1, 28)),

      ("20010332", null, null),
      ("20010331", "20010331", new Date(101, 2, 31)),

      ("20010431", null, null),
      ("20010430", "20010430", new Date(101, 3, 30)),

      ("20010532", null, null),
      ("20010531", "20010531", new Date(101, 4, 31)),

      ("20010631", null, null),
      ("20010630", "20010630", new Date(101, 5, 30)),

      ("20010732", null, null),
      ("20010731", "20010731", new Date(101, 6, 31)),

      ("20010832", null, null),
      ("20010831", "20010831", new Date(101, 7, 31)),

      ("20010931", null, null),
      ("20010930", "20010930", new Date(101, 8, 30)),

      ("20011032", null, null),
      ("20011031", "20011031", new Date(101, 9, 31)),

      ("20011131", null, null),
      ("20011130", "20011130", new Date(101, 10, 30)),

      ("20011232", null, null),
      ("20011231", "20011231", new Date(101, 11, 31))

    )

  val testDates =
    Table("dates",
      (1L to 3650L)
        .map(_ * 86400000L)
        .map(x => (new Date(x), expectedOutputStringFormatter.format(new Date(x))))
    )

  val conversionTypes =
    Table(
      ("inputFormat", "parseFormats"),
      ("MM-dd-yyyy", Seq(DateParsePattern.HyphenSeparator.MM_dd_yyyy)),
      ("dd-MM-yyyy", Seq(DateParsePattern.HyphenSeparator.dd_MM_yyyy)),
      ("yyyy-MM-dd", Seq(DateParsePattern.HyphenSeparator.yyyy_MM_dd)),
      ("MM/dd/yyyy", Seq(DateParsePattern.ForwardSlashSeparator.MM_dd_yyyy)),
      ("dd/MM/yyyy", Seq(DateParsePattern.ForwardSlashSeparator.dd_MM_yyyy)),
      ("yyyy/MM/dd", Seq(DateParsePattern.ForwardSlashSeparator.yyyy_MM_dd)),
      ("MMddyyyy", Seq(DateParsePattern.NoSeparator.MM_dd_yyyy)),
      ("ddMMyyyy", Seq(DateParsePattern.NoSeparator.dd_MM_yyyy)),
      ("yyyyMMdd", Seq(DateParsePattern.NoSeparator.yyyy_MM_dd)),
      ("MM dd yyyy", Seq(DateParsePattern.SpaceSeparator.MM_dd_yyyy)),
      ("dd MM yyyy", Seq(DateParsePattern.SpaceSeparator.dd_MM_yyyy)),
      ("yyyy MM dd", Seq(DateParsePattern.SpaceSeparator.yyyy_MM_dd)),
      ("MM\\dd\\yyyy", Seq(DateParsePattern.BackSlashSeparator.MM_dd_yyyy)),
      ("dd\\MM\\yyyy", Seq(DateParsePattern.BackSlashSeparator.dd_MM_yyyy)),
      ("yyyy\\MM\\dd", Seq(DateParsePattern.BackSlashSeparator.yyyy_MM_dd)),
    )

  behavior of "parseAsFormattedStringType"

  it should "produce the expected result with some hand-picked dates" in {
    forAll(inputAndExpectedResult) { (input: String, expected: String, date: Date) =>

      val df = testSparkSession.sparkContext.parallelize(Seq(input)).toDF("test_date")

      val dateColumn = SafeDates.parseAsFormattedStringType(
        col("test_date"),
        null,
        DateParsePattern.NoSeparator.yyyy_MM_dd, DateParsePattern.ForwardSlashSeparator.MM_dd_yyyy)

      println("Try match: " + input)
      val result = df.select(dateColumn).first().getAs[String](0)
      println("Spark Result: " + result + " should match " + expected)
      result should be(expected)
    }
  }

  it should "parse two digit years according to the threshold of -1" in {
    val df = testSparkSession.sparkContext.parallelize(Seq("6-30-20")).toDF("test_date")

    val dateColumn = SafeDates.parseAsFormattedStringType(
      col("test_date"),
      null,
      DateParsePattern.HyphenSeparator.M_d_yy(-1)).alias("out_date")

    val result = df.select(
      dateColumn,
    ).first().getAs[String](0)

    result should be("20200630")
  }

  it should "parse two digit years according to the threshold of 50" in {
    val df = testSparkSession.sparkContext.parallelize(Seq(("6-30-50"), ("6-30-51"))).toDF("test_date")

    val dateColumn = SafeDates.parseAsFormattedStringType(
      col("test_date"),
      null,
      DateParsePattern.HyphenSeparator.M_d_yy(50)).alias("out_date")

    val outcome = df.select(
      dateColumn
    )

    val expected = testSparkSession.sparkContext.parallelize(Seq(("20500630"), ("19510630"))).toDF("out_date")

    compareDataFrames(outcome, expected, Seq("out_date"))
  }

  behavior of "String AND Date parsers can use multiple parsing strategies"

  it should "return the expected string for ten years of input dates with two different formats used" in {
    val expectedOutputStringFormatter = new SimpleDateFormat("yyyyMMdd")
    val inputStringFormatter = new SimpleDateFormat("MM/dd/yyyy")
    val inputStringFormatter2 = new SimpleDateFormat("dd-MM-yyyy")

    val data = (1L to 3650L)
      .map(_ * 86400000L)
      .map(x => new Date(x))
      .map(x => (x,
        if (x.getDay % 2 == 0) inputStringFormatter.format(x) else inputStringFormatter2.format(x),
        expectedOutputStringFormatter.format(x)))

    val df = testSparkSession.sparkContext.parallelize(data).toDF("id", "input_date", "output_date")

    val dateStringColumn = SafeDates.parseAsFormattedStringType(
      col("input_date"),
      null, DateParsePattern.ForwardSlashSeparator.MM_dd_yyyy, DateParsePattern.HyphenSeparator.dd_MM_yyyy)
      .alias("output_date_string")

    val dateColumn = SafeDates.parseAsDateType(
      col("input_date"),
      null, DateParsePattern.ForwardSlashSeparator.MM_dd_yyyy, DateParsePattern.HyphenSeparator.dd_MM_yyyy)
      .alias("output_date_date")

    val result = df.select(col("id"), dateColumn, dateStringColumn)

    val expected = df.select(
      col("id"),
      col("id").alias("output_date_date"),
      col("output_date").alias("output_date_string")
    )

    println("outcome")
    result.printSchema()

    println("expected")
    expected.printSchema()

    compareDataFrames(result, expected, Seq("id"))
  }

  behavior of "parseAsFormattedStringType with all supported formats"

  it should "successfully convert each input string to the expected string" in {
    forAll(testDates) { (dates: Seq[(Date, String)]) =>
      forAll(conversionTypes) { (inputFormat: String, parseFormats: Seq[DynamicDateFormat]) => {

        val inputStringFormatter = new SimpleDateFormat(inputFormat)

        println(s"Testing ${dates.size} dates using input format $inputFormat and parsing formats:")
        parseFormats.foreach(println)

        val df = testSparkSession.sparkContext.parallelize(dates.map(x => (x._1.getTime, inputStringFormatter.format(x._1), x._2)))
          .toDF("id", "input_date", "expected_date")

        val dateColumn = SafeDates.parseAsFormattedStringType(
          col("input_date"),
          null, parseFormats: _*)
          .alias("expected_date")

        val outcome = df.select(col("id"), dateColumn)

        compareDataFrames(outcome, df.select("id", "expected_date"), Seq("id"))
        println("******* Complete *******")
      }
      }
    }
  }

  behavior of "parseAsDateType with all supported formats"

  it should "successfully convert each input string to the expected date" in {
    forAll(testDates) { (dates: Seq[(Date, String)]) =>
      forAll(conversionTypes) { (inputFormat: String, parseFormats: Seq[DynamicDateFormat]) => {

        val inputStringFormatter = new SimpleDateFormat(inputFormat)

        println(s"Testing ${dates.size} dates using input format $inputFormat and parsing formats:")
        parseFormats.foreach(println)

        val df = testSparkSession.sparkContext.parallelize(dates.map(x => (x._1.getTime, inputStringFormatter.format(x._1), x._1)))
          .toDF("id", "input_date", "expected_date")

        val dateColumn = SafeDates.parseAsDateType(
          col("input_date"),
          null, parseFormats: _*)
          .alias("expected_date")

        val outcome = df.select(col("id"), dateColumn)

        compareDataFrames(outcome, df.select("id", "expected_date"), Seq("id"))
        println("******* Complete *******")
      }
      }
    }
  }

  behavior of "parseAsDateType"

  it should "produce the expected result with some hand-picked dates" in {
    forAll(inputAndExpectedResult) { (input: String, expected: String, date: Date) =>

      val df = testSparkSession.sparkContext.parallelize(Seq(input)).toDF("test_date")

      val dateColumn = SafeDates.parseAsDateType(
        col("test_date"),
        null,
        DateParsePattern.NoSeparator.yyyy_MM_dd, DateParsePattern.ForwardSlashSeparator.MM_dd_yyyy)

      println("Try match: " + input)
      val result = df.select(dateColumn).first().getAs[Date](0)
      println("Spark Result: " + result + " should match " + date)
      result should be(date)
    }
  }
}




