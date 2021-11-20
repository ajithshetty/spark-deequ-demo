// Databricks notebook source
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.Completeness
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.google.common.io.Files
import java.io.File
import com.amazon.deequ.{VerificationSuite, VerificationResult}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date, to_timestamp}
import org.apache.spark.sql.types.TimestampType
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.profiles._

// COMMAND ----------

val columns = Seq("id","name","address","citizenship","travel_count")

    // The toy data on which we will compute metrics
val data_vals = Seq(
      (1, "user1", "USA", "NO", 0),
      (2, "user2", "UK", "YES", 0),
      (3, null, null, "YES", 5),
      (4, "user4", "CANADA", null, 10),
      (5, "user5", null, "NA", 12))
val rdd = spark.sparkContext.parallelize(data_vals)
val data=spark.createDataFrame(rdd).toDF(columns:_*)
display(data)

// COMMAND ----------

    val session = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    session.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))

// COMMAND ----------

// MAGIC %fs rm -r dbfs:/data/deeque_output/metrics.json

// COMMAND ----------

    // The repository which we will use to stored and load computed metrics; we use the local disk,
    // but it also supports HDFS and S3
    val repository: MetricsRepository =
      FileSystemMetricsRepository(session, "dbfs:/data/deeque_output/metrics.json")

    // The key under which we store the results, needs a timestamp and supports arbitrary
    // tags in the form of key-value pairs
    val tag_name=java.time.LocalDate.now.toString
    val resultKey = ResultKey(System.currentTimeMillis(), Map("tag" -> tag_name))


// COMMAND ----------

val verificationResult: VerificationResult = {    VerificationSuite()
      .onData(data)
      // Some integrity checks
      .addCheck(
        Check(CheckLevel.Error, "integrity checks")
        .hasSize(_ > 5)
        .isComplete("id")
        .isComplete("name")
        .isContainedIn("citizenship", Array("YES", "NO"))
        .isNonNegative("travel_count"))
      .run()
}
// convert check results to a Spark data frame
val resultDataFrame = checkResultsAsDataFrame(spark, verificationResult)
display(resultDataFrame)

// COMMAND ----------

VerificationSuite()
      .onData(data)
      // Some integrity checks
            .addCheck(
        Check(CheckLevel.Error, "integrity checks")
        .hasSize(_ >= 5)
        .isComplete("id")
        .isComplete("name")
        .isContainedIn("citizenship", Array("YES", "NO"))
        .isNonNegative("travel_count"))
      // We want to store the computed metrics for the checks in our repository
      .useRepository(repository)
      .saveOrAppendResult(resultKey)
      .run()

// COMMAND ----------

    // We can now retrieve the metrics from the repository in different ways, e.g.:
    // We can load the metric for a particular analyzer stored under our result key:
    val completenessOfName = repository
      .loadByKey(resultKey).get
      .metric(Completeness("name")).get

    println(s"The completeness of the name column is: $completenessOfName")

    // We can query the repository for all metrics from the last 10 minutes and get them as json
    val json = repository.load()
      .after(System.currentTimeMillis() - 10000)
      .getSuccessMetricsAsJson()

    println(s"Metrics from the last 10 minutes:\n$json")

// COMMAND ----------

    // Finally we can also query by tag value and retrieve the result in the form of a dataframe
    repository.load()
      .withTagValues(Map("tag" -> tag_name))
      .getSuccessMetricsAsDataFrame(session)
      .withColumn("dataset_date", ($"dataset_date" / 1000).cast(TimestampType))
      .show(false)

// COMMAND ----------

data.show(false)

// COMMAND ----------

val result = ColumnProfilerRunner()
  .onData(data)
  .run()

// COMMAND ----------

result.profiles.foreach { case (colName, profile) =>
  println(s"Column '$colName':\n " +
    s"\tcompleteness: ${profile.completeness}\n" +
    s"\tapproximate number of distinct values: ${profile.approximateNumDistinctValues}\n" +
    s"\tdatatype: ${profile.dataType}\n")
}

// COMMAND ----------


