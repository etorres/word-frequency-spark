package es.eriktorr.katas

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

import com.holdenkarau.spark.testing.DataframeGenerator.arbitraryDataFrameWithCustomFields
import com.holdenkarau.spark.testing.DatasetGenerator.genDataset
import com.holdenkarau.spark.testing.{Column, SharedSparkContext}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalacheck.Prop.forAll
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class WordFrequencyApplicationTest extends FunSuite with SharedSparkContext with Checkers {

  test("converts lines of text into words reusing timestamps") {
    val sparkSession = SparkSession.builder().getOrCreate()

    val lowerTimestamp: Long = LocalDateTime.now().minusMinutes(5).toEpochSecond(ZoneOffset.UTC)
    val upperTimestamp: Long = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)

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
    import sparkSession.implicits._

    val lowerTimestamp: Long = LocalDateTime.now().minusMinutes(5).toEpochSecond(ZoneOffset.UTC)
    val upperTimestamp: Long = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)

    val timestampGen: Gen[Timestamp] = {
      Gen.choose(lowerTimestamp, upperTimestamp).map(new Timestamp(_))
    }

    val datasetGenerator: Gen[Dataset[Word]] =
      genDataset[Word](sparkSession.sqlContext) {
        val generator: Gen[Word] = for {
          word <- Arbitrary.arbitrary[String]
          timestamp <- timestampGen
        } yield Word(word, timestamp)

        generator
      }

    val property =
      forAll(datasetGenerator) {
        dataset => dataset.map(_.word).count() == dataset.count()
      }

    check(property)

    fail("feature under development") // TODO
  }

}
