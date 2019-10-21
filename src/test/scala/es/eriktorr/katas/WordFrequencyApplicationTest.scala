package es.eriktorr.katas

import com.holdenkarau.spark.testing.DataframeGenerator.arbitraryDataFrameWithCustomFields
import com.holdenkarau.spark.testing.{Column, SharedSparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class WordFrequencyApplicationTest extends FunSuite with SharedSparkContext with Checkers {

  test("converts lines of text into words reusing timestamps") {
    val sparkSession = SparkSession.builder().getOrCreate()

    val lowerTimestamp: Long = 1571501812L
    val upperTimestamp: Long = 1571652348L

    val schema = StructType(List(StructField("key", StringType), StructField("value", StringType), StructField("timestamp", LongType)))
    val keyGenerator = new Column("key", Gen.const(null))
    val valueGenerator = new Column("value", Gen.oneOf(
      "precipitous lurid dwelling shudder thrilling",
      "lurid precipitous thrilling",
      "dwelling lurid precipitous"))
    val timestampGenerator = new Column("timestamp", Gen.choose(lowerTimestamp, upperTimestamp))
    val dataFrameGenerator = arbitraryDataFrameWithCustomFields(sparkSession.sqlContext, schema)(keyGenerator, valueGenerator, timestampGenerator)

    val wordStreamFrequencyCounter = new WordStreamFrequencyCounter(
      "bootstrapServers",
      "inTopics",
      "outTopics",
      "checkpointLocation")

    val property =
      forAll(dataFrameGenerator.arbitrary) {
        dataFrame => dataFrame.schema === schema &&
          wordStreamFrequencyCounter.wordsFrom(dataFrame).filter(
            "(word != 'precipitous' AND word != 'lurid' AND word != 'dwelling' AND word != 'shudder' AND word != 'thrilling') OR " +
              s"(timestamp > ${upperTimestamp.toString} OR timestamp < ${lowerTimestamp.toString})").count() == 0
      }

    check(property)
  }

  test("find the 10 most common words in a windowed stream") {
    val sparkSession = SparkSession.builder().getOrCreate()

    

    fail("feature under development")
  }

}
