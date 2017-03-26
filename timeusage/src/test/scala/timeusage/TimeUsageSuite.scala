package timeusage

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen, Matchers}
import timeusage.TimeUsage.timeUsageGrouped

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with Matchers with GivenWhenThen with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Time Usage")
    .config("spark.master", "local[2]")
    .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
    .getOrCreate()

  lazy val (columns, initDf) = TimeUsage.read("/timeusage/atussum.csv")

  test("should classify columns") {
    When("classifiedColumns is invoked")
    val (primaryNeedsColumns, workColumns, otherColumns) = TimeUsage.classifiedColumns(columns)

    Then("columns are properly classified")
    primaryNeedsColumns should have size 55
    workColumns should have size 23
    otherColumns should have size 346
  }

  test("should create time usage summary") {
    Given("a classified columns")
    val (primaryNeedsColumns, workColumns, otherColumns) = TimeUsage.classifiedColumns(columns)

    When("timeUsageSummary is invoked")
    val summaryDf = TimeUsage.timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)

    Then("summary is created")
    summaryDf.show(false)
  }

  test("should create time usage grouped") {
    Given("a summary df")
    val (primaryNeedsColumns, workColumns, otherColumns) = TimeUsage.classifiedColumns(columns)
    val summaryDf = TimeUsage.timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)

    When("timeUsageGrouped is invoked")
    val finalDf = TimeUsage.timeUsageGrouped(summaryDf)
    val finalTypedDf = timeUsageGrouped(summaryDf)

    Then("a grouped time usage is produced")
    finalDf.printSchema()
    finalDf.show(false)

    finalTypedDf.printSchema()
    finalTypedDf.show(false)
  }
}