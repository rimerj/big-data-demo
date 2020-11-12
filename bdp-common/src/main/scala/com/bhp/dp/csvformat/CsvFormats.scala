package com.bhp.dp.csvformat


import org.apache.spark.sql.DataFrameReader

object CsvFormats {
  def headerCommaDelimitedDoubleQuoteUTF8(reader: DataFrameReader): DataFrameReader = reader
    .options(Map[String,String](
      "header" -> "true",
      "inferSchema" -> "false",
      "delimiter" -> ",",
      "quote" -> "\"",
      "encoding" -> "UTF-8")
    )
}
