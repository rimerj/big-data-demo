package com.bhp.dp.utils

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source

object JarResources {
  /**
   * Returns the contents of a resource file in a String
   *
   * @param resourceName
   * @return String contents of resourceName loaded from resources of the JAR
   */
  def loadRawResource(resourceName: String): String = {
    val path = if (resourceName.startsWith("/")) resourceName else s"/$resourceName"
    val inputStream = JarResources.getClass.getResourceAsStream(path)
    Source.fromInputStream(inputStream).mkString
  }

  /**
   * Useful for consuming CSVs from src/main/resources
   *
   * @param resourceName
   * @param sparkSession
   * @return Dataset[String] of the file contents
   */
  def parallelizeResource(resourceName: String, sparkSession: SparkSession): Dataset[String] = {
    import sparkSession.implicits._
    val content = loadRawResource(resourceName)
    sparkSession.sparkContext.parallelize(content.lines.toList).toDS
  }
}
