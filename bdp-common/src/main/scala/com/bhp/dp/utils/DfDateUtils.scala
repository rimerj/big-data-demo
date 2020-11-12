package com.bhp.dp.utils

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.sql.Date

import org.apache.spark.sql.types.IntegerType

object DateParsePattern {
  object ForwardSlashSeparator {
    def M_d_yy(twoDigitDateThreshold: Int): DynamicDateFormat = {
      createDateParsePattern("M/d/yy", Some(twoDigitDateThreshold))
    }

    val yyyy_MM_dd = createDateParsePattern("yyyy/MM/dd")
    val MM_dd_yyyy = createDateParsePattern("MM/dd/yyyy")
    val dd_MM_yyyy = createDateParsePattern("dd/MM/yyyy")
  }

  object BackSlashSeparator {
    def M_d_yy(twoDigitDateThreshold: Int): DynamicDateFormat = {
      createDateParsePattern("M\\d\\yy", Some(twoDigitDateThreshold))
    }

    val yyyy_MM_dd = createDateParsePattern("yyyy\\MM\\dd")
    val MM_dd_yyyy = createDateParsePattern("MM\\dd\\yyyy")
    val dd_MM_yyyy = createDateParsePattern("dd\\MM\\yyyy")
  }

  object HyphenSeparator {
    /**
     * You must provide the information on how the two digit date it to be interpreted. For example:
     *
     * -1 means treat all two digits as needing the '20' prefix
     *
     * '06-30-50' => '20500630'
     *
     * '06-30-51' => '20510630'
     *
     * 50 means treat any two digit values up and including 50 as needing the '20' prefix otherwise prefix with '19'
     *
     * '06-30-50' => '20500630'
     *
     * '06-30-51' => '19510630'
     *
     * @param twoDigitDateThreshold
     * @return
     */
    def M_d_yy(twoDigitDateThreshold: Int): DynamicDateFormat = {
      createDateParsePattern("M-d-yy", Some(twoDigitDateThreshold))
    }

    val yyyy_MM_dd = createDateParsePattern("yyyy-MM-dd")
    val MM_dd_yyyy = createDateParsePattern("MM-dd-yyyy")
    val dd_MM_yyyy = createDateParsePattern("dd-MM-yyyy")
  }

  object SpaceSeparator {
    def M_d_yy(twoDigitDateThreshold: Int): DynamicDateFormat = {
      createDateParsePattern("M d yy", Some(twoDigitDateThreshold))
    }

    val yyyy_MM_dd = createDateParsePattern("yyyy MM dd")
    val MM_dd_yyyy = createDateParsePattern("MM dd yyyy")
    val dd_MM_yyyy = createDateParsePattern("dd MM yyyy")
  }

  object NoSeparator {
    def M_d_yy(twoDigitDateThreshold: Int): DynamicDateFormat = {
      createDateParsePattern("Mdyy", Some(twoDigitDateThreshold))
    }

    val yyyy_MM_dd = createDateParsePattern("yyyyMMdd")
    val MM_dd_yyyy = createDateParsePattern("MMddyyyy")
    val dd_MM_yyyy = createDateParsePattern("ddMMyyyy")
  }

  private final val yyyyRegex = "([1-2][0-9][0-9][0-9])"
  private final val yyRegex = "([0-9][0-9])"
  private final val mmRegex = "((?:0[1-9]|1[012]))"
  private final val ddRegex = "((?:0[1-9]|1[0-9]|2[0-9]|3[0-1]))"
  private final val mRegex = "([1-9]|[1][0-2])"
  private final val dRegex = "([1-9]|[1-2][0-9]|[3][0-1])"

  final def createDateParsePattern(datePattern: String, twoDigitDateThreshold: Option[Int] = None): DynamicDateFormat = {
    // Convert the date string pattern to a regular expression
    val regexPattern = datePattern
      .replace("yyyy", yyyyRegex)
      .replace("MM", mmRegex)
      .replace("dd", ddRegex)
      .replace("yy", yyRegex)
      .replace("M", mRegex)
      .replace("d", dRegex)
      .replace("\\", "\\\\")

    // figure out the order in which the date elements appear
    val datePartIndexes = Seq(
      "year" -> regexPattern.indexOf(yyyyRegex),
      "month" -> regexPattern.indexOf(mmRegex),
      "day" -> regexPattern.indexOf(ddRegex),
      "year" -> regexPattern.indexOf(yyRegex),
      "month" -> regexPattern.indexOf(mRegex),
      "day" -> regexPattern.indexOf(dRegex)
    )
      .filter(_._2 >= 0)
      .sortBy(_._2)
      .zipWithIndex
      .map(x => x._1._1 -> (x._2 + 1))
      .toMap

    DynamicDateFormat(
      regexPattern,
      datePartIndexes("year"),
      datePartIndexes("month"),
      datePartIndexes("day"),
      twoDigitDateThreshold
    )
  }
}

case class DynamicDateFormat(
                              regularExpression: String,
                              yearGroupIndex: Int,
                              monthGroupIndex: Int,
                              dayGroupIndex: Int,
                              twoDigitDateThreshold: Option[Int]
                            )

object SafeDates {

  final def parseAsDateType(
                             sourceColumn: Column,
                             fallbackValue: Date,
                             dateFormat: DynamicDateFormat*): Column = {
    to_date(
      unix_timestamp(
        dateFormat match {
          case Seq(head) => coalesce(getDateSafeInternal(sourceColumn, head), lit(fallbackValue))
          case dateFormats =>
            coalesce(
              dateFormats.map(
                getDateSafeInternal(sourceColumn, _)
              ) ++ Seq(lit(fallbackValue)): _*
            )
        },
        "yyyyMMdd"
      ).cast("TIMESTAMP")
    )
  }

  final def parseAsFormattedStringType(
                                        sourceColumn: Column,
                                        fallbackValue: String,
                                        dateFormat: DynamicDateFormat*
                                      ): Column = {
    dateFormat match {
      case Seq(head) => coalesce(getDateSafeInternal(sourceColumn, head), lit(fallbackValue))
      case dateFormats =>
        coalesce(
          dateFormats.map(
            getDateSafeInternal(sourceColumn, _)
          ) ++ Seq(lit(fallbackValue)): _*
        )
    }
  }

  private final def checkMonthAndDays(year: Column, month: Column, day: Column): Column = {

    val yearInt = year.cast(IntegerType)

    /* if (year is not divisible by 4) then (it is a common year)
    else if (year is not divisible by 100) then (it is a leap year)
    else if (year is not divisible by 400) then (it is a common year)
    else (it is a leap year) */
    val daysInFeb = when(yearInt.mod(4) =!= lit(0), 28)
      .when(yearInt.mod(100) =!= lit(0), 29)
      .when(yearInt.mod(400) =!= lit(0), 28)
      .otherwise(29)

    day.cast(IntegerType) <= when(month.cast(IntegerType).isin(1, 3, 5, 7, 8, 10, 12), lit(31))
      .when(month.cast(IntegerType).isin(4, 6, 9, 11), lit(30))
      .otherwise(daysInFeb)
  }

  // format: off
  private final def getDateSafeInternal(
                                         sourceColumn: Column,
                                         dateFormat: DynamicDateFormat): Column = {

    val yearColumn = dateFormat.twoDigitDateThreshold
      .foldLeft(regexp_extract(sourceColumn, dateFormat.regularExpression, dateFormat.yearGroupIndex))((acc, cur) => {
        when(lit(cur) === lit(-1), concat(lit("20"), acc))
          .when(acc.cast(IntegerType) <= lit(cur), concat(lit("20"), acc))
            .otherwise(concat(lit("19"), acc))
      })

    when(
      // test to see if the date string has the right pattern
      sourceColumn.rlike(dateFormat.regularExpression)
        // test to see if the number of days is correct for the month (includes february and leap years)
        && checkMonthAndDays(
        //regexp_extract(sourceColumn, dateFormat.regularExpression, dateFormat.yearGroupIndex),
        yearColumn,
        regexp_extract(sourceColumn, dateFormat.regularExpression, dateFormat.monthGroupIndex),
        regexp_extract(sourceColumn, dateFormat.regularExpression, dateFormat.dayGroupIndex)
      ),
      concat(
        //regexp_extract(sourceColumn, dateFormat.regularExpression, dateFormat.yearGroupIndex),
        yearColumn,
        lpad(regexp_extract(sourceColumn, dateFormat.regularExpression, dateFormat.monthGroupIndex), 2, "0"),
        lpad(regexp_extract(sourceColumn, dateFormat.regularExpression, dateFormat.dayGroupIndex), 2, "0")
      )
    ).otherwise(lit(null))
  }
  // format: on
}
