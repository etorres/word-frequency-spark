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

  test("test multiple columns generators") {
    val sparkSession = SparkSession.builder().getOrCreate()

    val schema = StructType(List(StructField("key", StringType), StructField("value", StringType), StructField("timestamp", LongType)))
    val keyGenerator = new Column("key", Gen.const(null))
    val valueGenerator = new Column("value", Gen.oneOf(
      "precipitous lurid dwelling shudder thrilling",
      "lurid precipitous thrilling",
      "dwelling lurid precipitous"))
    val timestampGenerator = new Column("timestamp", Gen.choose(1571501812L, 1900000000L))
    val dataFrameGenerator = arbitraryDataFrameWithCustomFields(sparkSession.sqlContext, schema)(keyGenerator, valueGenerator, timestampGenerator)

    val wordStreamFrequencyCounter = new WordStreamFrequencyCounter(
      "bootstrapServers",
      "inTopics",
      "outTopics",
      "checkpointLocation")

    val property =
      forAll(dataFrameGenerator.arbitrary) {
        dataFrame => dataFrame.schema === schema &&
          wordStreamFrequencyCounter.wordsFrom(dataFrame).filter("(key != null) OR " +
            "(value != 'precipitous' AND value != 'lurid' AND value != 'dwelling' AND value != 'shudder' AND value != 'thrilling') OR " +
            "(timestamp > 1900000000 OR timestamp < 1571501812)").count() == 0
      }

    check(property)
  }

}
