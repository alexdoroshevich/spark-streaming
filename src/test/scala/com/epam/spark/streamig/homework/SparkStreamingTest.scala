package com.epam.spark.streamig.homework

import com.epam.spark.streamig.homework.domain.{HotelState, HotelStateKey}
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.Row
import org.scalatest.FunSpec

class SparkStreamingTest extends FunSpec with SparkSessionTestWrapper with DatasetComparer {

  import spark.implicits._

  it("should join hotel_weather and calculate states") {
    val exdepiaDF = Seq(
      (125L, "2018-01-01", "2018-01-01", 2),
      (125L, "2018-02-01", "2018-01-01", 0),
      (124L, "2018-01-01", "2018-01-02", 2),
      (126L, "2018-01-01", "2018-01-05", 0),
      (123L, "2018-01-01", "2018-01-15", 2),
      (127L, "2018-01-01", "2018-01-30", 2)
    ).toDF("expedia_hotel_id", "srch_ci", "srch_co", "srch_children_cnt")

    val hotelWeatherDF = Seq(
      (7.0, "2018-01-01", 122L),
      (15.0, "2018-01-01", 123L),
      (18.0, "2018-01-01", 124L),
      (19.0, "2018-01-01", 125L),
      (25.0, "2018-02-01", 125L),
      (8.0, "2018-01-01", 126L),
      (8.0, "2018-01-01", 127L)
    ).toDF("avg_tmpr_c", "wthr_date", "hotel_id")

    val expectedStateDF = Seq(
      (125L, true, 1, 0, 0, 0, 0),
      (125L, false, 1, 0, 0, 0, 0),
      (124L, true, 0, 1, 0, 0, 0),
      (126L, false, 0, 0, 1, 0, 0),
      (123L, true, 0, 0, 0, 1, 0),
      (127L, true, 0, 0, 0, 0, 1)
    ).toDF("expedia_hotel_id", "with_children", "erroneous_data", "short_stay", "standart_stay",
      "standart_extended_stay", "long_stay")

    val actualStateDF = SparkStreaming.joinHotelWeatherAndCalculateStates(exdepiaDF, hotelWeatherDF)

    assertSmallDatasetEquality(expectedStateDF, actualStateDF)
  }

  it("should map row to HotelState") {
    val row = Row(1234L, true, 0, 1, 0, 0, 0, "")
    val expectedHotelState = HotelState(HotelStateKey("1234", true), 0, 1, 0, 0, 0, "")
    val actualHotelState = SparkStreaming.mapToHotelState(row)

    assertResult(expectedHotelState)(actualHotelState)
  }

  it("should populate ResultType to HotelState") {
    val actualHotelState = HotelState(HotelStateKey("1234", true), 0, 1, 0, 0, 0, "")

    val hotelExpected = HotelState(HotelStateKey("1234", true), 0, 1, 0, 0, 0, "short_stay")

    SparkStreaming.populateResultType(actualHotelState)

    assertResult(hotelExpected)(actualHotelState)
  }

  it("should populate resultType to HotelState") {
    val actualHotelState = HotelState(HotelStateKey("1234", true), 0, 1, 0, 0, 0, "")

    val hotelExpected = HotelState(HotelStateKey("1234", true), 0, 1, 0, 0, 0, "short_stay")

    SparkStreaming.populateResultType(actualHotelState)

    assertResult(hotelExpected)(actualHotelState)
  }
}
