package com.bhp.dp.utils

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object DfUtils {
  final def removeNullsFromDf(dfResults: DataFrame): DataFrame = dfResults.na.fill("")

  /**
   * Takes a column with a date stored as a string ex. 20201023, 1/1/2020, converts it to date based on expected input format
   * exports it back as the outputFormat
   *
   * @param dateColumn
   * @param inputFormat
   * @param outputFormat ...exports it back as the outputFormat
   * @return
   */
  final def formatStringyDate(dateColumn: Column, inputFormat: String, outputFormat: String): Column =
    date_format(getDateSafe(dateColumn, inputFormat), outputFormat)

  /**
   * TO_DATE(CAST(UNIX_TIMESTAMP(SUBSTRING_INDEX(CREDENTIALING_BOARD_MEETING_DATE, ' ', 1), 'm/d/yyyy') AS TIMESTAMP))
   */
  final def getDateSafe(sourceColumn: Column, datePattern: String): Column = {
    to_date(
      unix_timestamp(
        substring_index(sourceColumn, " ", 1), datePattern
      ).cast("TIMESTAMP")
    )
  }
}