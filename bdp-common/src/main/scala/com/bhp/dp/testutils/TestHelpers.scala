package com.bhp.dp.testutils

// ZZ:  DO NOT import any junit/scalatest dependencies here
// this is in the main part of the code, so importing junit stuff will absolutely pollute our classpath
// the reason this is here is because these functions need to be shared across projects
// they are possibly useful for normal functionality as well, but are mainly used for tests

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.co.gresearch.spark.diff._

trait TestHelpers {
  final val bdpDefaultDiffOptions = DiffOptions.default
    .withDeleteDiffValue("Absent")
    .withChangeDiffValue("!=")
    .withInsertDiffValue("Unexpected")
    .withNochangeDiffValue("✓")  // <-- wth is this symbol? do we really want non-ascii in our code base?? :)
    .withLeftColumnPrefix("Expected")
    .withRightColumnPrefix("Outcome")

  final val SparseDiffResults = bdpDefaultDiffOptions
    .withSparseMode(true)

  protected def compareDataFrames(outcome: DataFrame, expected: DataFrame, primaryKeyColumns: Seq[String], diffOptions: DiffOptions = bdpDefaultDiffOptions): Unit = {
    val results = expected.diff(outcome, diffOptions, primaryKeyColumns: _*)

    val differences = results.filter(col(diffOptions.diffColumn) =!= lit(diffOptions.nochangeDiffValue))
      .count()

    try {
      assert(differences == 0, "Differences Detected between outcome and expected")
    }
    catch {
      case e: AssertionError => {
        results.show(1000, false)
        throw e
      }
    }
  }

  protected def psvStringToDf(sparkSession: SparkSession, csv: String): DataFrame = {
    import sparkSession.implicits._

    sparkSession.read.option("header", true).option("inferSchema", false).option("delimiter", "|").csv(
      sparkSession.sparkContext.parallelize(csv.lines.toList).toDS().cache()
    )
  }
  protected def csvStringToDf(sparkSession: SparkSession, csv: String): DataFrame = {
    import sparkSession.implicits._

    sparkSession.read.option("header", true).option("inferSchema", false).option("delimiter", ",").csv(
      sparkSession.sparkContext.parallelize(csv.lines.toList).toDS().cache()
    )
  }
}
