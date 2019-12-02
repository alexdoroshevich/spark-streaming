package com.epam.spark.streamig.homework

import com.epam.spark.streamig.homework.domain.{HotelState, HotelStateKey}
import org.apache.spark.sql.functions.{col, datediff, when}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types._
import org.apache.spark.sql._


object SparkStreaming {

  val EXPEDIA_STREAMING_INPUT = "hdfs://sandbox-hdp.hortonworks.com:8020/homework/spark/batching/result/srch_ci_year=2017"
  val EXPEDIA_BATCHING_INPUT = "hdfs://sandbox-hdp.hortonworks.com:8020/homework/spark/batching/result/srch_ci_year=2016"

  val HOTEL_STATE_CHECKPOINT = "hdfs://sandbox-hdp.hortonworks.com:8020/homework/spark/streaming/checkpoint"
  val HOTEL_STATE_OUTPUT = "hdfs://sandbox-hdp.hortonworks.com:8020/homework/spark/streaming/result"

  val EMPTY_RESULT_TYPE = ""

  val CSV_DELIMITER = ","

  val COL_HOTEL_ID = "hotel_id"
  val COL_EXPEDIA_HOTEL_ID = "expedia_hotel_id"
  val COL_SRCH_CI = "srch_ci"
  val COL_SRCH_CO = "srch_co"
  val COL_WTHR_DATE = "wthr_date"
  val COL_SRCH_CHILDREN_CNT = "srch_children_cnt"
  val COL_AVG_TMPR_C = "avg_tmpr_c"

  val COL_WITH_CHILDREN = "with_children"
  val COL_ERRONEOUS_DATA = "erroneous_data"
  val COL_SHORT_STAY = "short_stay"
  val COL_STANDART_STAY = "standart_stay"
  val COL_STANDART_EXTENDED_STAY = "standart_extended_stay"
  val COL_LONG_STAY = "long_stay"
  val COL_STAY_DURATION = "stay_duration"

  val STAY_TYPES = List(COL_ERRONEOUS_DATA, COL_SHORT_STAY, COL_STANDART_STAY, COL_STANDART_EXTENDED_STAY, COL_LONG_STAY)

  var initialHotelStateMap: Map[HotelStateKey, HotelState] = Map.empty[HotelStateKey, HotelState]

  implicit val hotelStateEncoder: Encoder[HotelState] = Encoders.kryo[HotelState]

  val STATE_DF_SCHEMA = StructType(
    Array(
      StructField("hotel_id", StringType, nullable = false),
      StructField("erroneous_data", StringType, nullable = false).withComment("null, more than month(30 days), less than or equal to 0"),
      StructField("short_stay", StringType, nullable = false).withComment("1-3 days"),
      StructField("standart_stay", StringType, nullable = false).withComment("4-7 days"),
      StructField("standart_extended_stay", StringType, nullable = false).withComment("1-2 weeks"),
      StructField("long_stay", StringType, nullable = false).withComment("2-4 weeks (less than month)")
    ))

  val HOTEL_WEATHER_SCHEMA = StructType(
    Array(
      StructField("avg_tmpr_c", DoubleType, nullable = false),
      StructField("wthr_date", StringType, nullable = false),
      StructField("hotel_id", LongType, nullable = false)
    ))

  val EXPEDIA_SCHEMA = new StructType()
    .add("id", "long")
    .add("date_time", "string")
    .add("site_name", "integer")
    .add("posa_continent", "integer")
    .add("user_location_country", "integer")
    .add("user_location_region", "integer")
    .add("user_location_city", "integer")
    .add("orig_destination_distance", "double")
    .add("user_id", "integer")
    .add("is_mobile", "integer")
    .add("is_package", "integer")
    .add("channel", "integer")
    .add("srch_ci", "string")
    .add("srch_co", "string")
    .add("srch_adults_cnt", "integer")
    .add("srch_children_cnt", "integer")
    .add("srch_rm_cnt", "integer")
    .add("srch_destination_id", "integer")
    .add("srch_destination_type_id", "integer")
    .add("hotel_id", "long")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("sparl-streaming-example")
      .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
      .getOrCreate()

    val hotelWeatherDF = getHotelWeatherDFKafka(spark)
    //hotelWeatherDF.show()

    initialHotelStateMap = getInitialHotelStateMap(spark, hotelWeatherDF)

    startSparkStreaming(spark, hotelWeatherDF)

    spark.stop()
  }

  def getHotelWeatherDFKafka(spark: SparkSession): Dataset[Row] = {
    val hotelWeatherRows = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribePattern", "hive_hotel_weather_topic")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .map(row => {
        val value = row.get(1)
        val values = value.toString.replaceAll("\"", "").split(",")
        Row(values(3).toDouble, values(4), values(6).toLong)
      })(Encoders.kryo[Row])
      .collectAsList()

    val hotelWeatherDF = spark.createDataFrame(hotelWeatherRows, HOTEL_WEATHER_SCHEMA)
    val hotelWeatherCleanedDF = filterOutInvalidData(hotelWeatherDF)

    hotelWeatherCleanedDF
  }

  def filterOutInvalidData(hotelWeatherDF: Dataset[Row]): Dataset[Row] = {

    val hotelWeatherDFCleaned = hotelWeatherDF.groupBy(COL_WTHR_DATE, COL_HOTEL_ID)
      .avg(COL_AVG_TMPR_C)
      .withColumnRenamed("avg(avg_tmpr_c)", COL_AVG_TMPR_C)
      .filter(col(COL_AVG_TMPR_C) > 0)

    hotelWeatherDFCleaned
  }

  def getInitialHotelStateMap(spark: SparkSession, hotelWeatherDF: Dataset[Row]): Map[HotelStateKey, HotelState] = {
    val expediaDF = spark.read
      .schema(EXPEDIA_SCHEMA)
      .parquet(EXPEDIA_BATCHING_INPUT)
      .withColumnRenamed(COL_HOTEL_ID, COL_EXPEDIA_HOTEL_ID)

    val initialHotelStateMap = joinHotelWeatherAndCalculateStates(expediaDF, hotelWeatherDF)
      .groupBy(COL_EXPEDIA_HOTEL_ID, COL_WITH_CHILDREN)
      .sum(COL_ERRONEOUS_DATA, COL_SHORT_STAY, COL_STANDART_STAY, COL_STANDART_EXTENDED_STAY, COL_LONG_STAY)
      .map(
        row => {
          val hotelState = mapToHotelState(row)
          (hotelState.hotelStateKey, hotelState)
        }
      )(Encoders.kryo[(HotelStateKey, HotelState)])
      .collect.toMap
    initialHotelStateMap
  }

  def startSparkStreaming(spark: SparkSession, hotelWeatherDF: Dataset[Row]): Unit = {
    val expediaStream: Dataset[Row] = spark.readStream
      .schema(EXPEDIA_SCHEMA)
      .parquet(EXPEDIA_STREAMING_INPUT)
      .withColumnRenamed(COL_HOTEL_ID, COL_EXPEDIA_HOTEL_ID)

    import spark.implicits._

    joinHotelWeatherAndCalculateStates(expediaStream, hotelWeatherDF)
      .map(row => {
        val hotelState = mapToHotelState(row)
        hotelState
      })
      .groupByKey(_.hotelStateKey)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout())(aggregateHotelState)
      .map(hotelState => {
        populateResultType(hotelState)
        hotelState
      })
      .map(hotelState => {
        //sorry for junior code
        val row: String = hotelState.hotelStateKey.hotelId + CSV_DELIMITER + hotelState.hotelStateKey.withChildren +
          CSV_DELIMITER + hotelState.erroneousData + CSV_DELIMITER + hotelState.shortStay + CSV_DELIMITER +
          hotelState.standartStay + CSV_DELIMITER + hotelState.standartExtendedStay + CSV_DELIMITER +
          hotelState.longStay + CSV_DELIMITER + hotelState.resultingType

        println(row)
        row
      })
      .writeStream
      .outputMode(OutputMode.Append())
      .format("csv")
      .option("checkpointLocation", HOTEL_STATE_CHECKPOINT)
      .option("path", HOTEL_STATE_OUTPUT)
      .start()
      .awaitTermination()
  }

  def joinHotelWeatherAndCalculateStates(expediaDF: Dataset[Row], hotelWeatherDF: Dataset[Row]): Dataset[Row] = {
    val stateDF = expediaDF
      .join(hotelWeatherDF, hotelWeatherDF(COL_HOTEL_ID) === expediaDF(COL_EXPEDIA_HOTEL_ID)
        && hotelWeatherDF(COL_WTHR_DATE) === expediaDF(COL_SRCH_CI))
      .withColumn(COL_STAY_DURATION, datediff(col(COL_SRCH_CO), col(COL_SRCH_CI)))
      .withColumn(COL_ERRONEOUS_DATA, when(col(COL_STAY_DURATION) <= 0 || col(COL_STAY_DURATION) > 30, 1).otherwise(0))
      .withColumn(COL_SHORT_STAY, when(col(COL_STAY_DURATION) >= 1 && col(COL_STAY_DURATION) <= 3, 1).otherwise(0))
      .withColumn(COL_STANDART_STAY, when(col(COL_STAY_DURATION) > 3 && col(COL_STAY_DURATION) <= 7, 1).otherwise(0))
      .withColumn(COL_STANDART_EXTENDED_STAY, when(col(COL_STAY_DURATION) > 7 && col(COL_STAY_DURATION) <= 14, 1).otherwise(0))
      .withColumn(COL_LONG_STAY, when(col(COL_STAY_DURATION) > 14 && col(COL_STAY_DURATION) <= 30, 1).otherwise(0))
      .withColumn(COL_WITH_CHILDREN, when(col(COL_SRCH_CHILDREN_CNT) > 0, true).otherwise(false))
      .select(COL_EXPEDIA_HOTEL_ID, COL_WITH_CHILDREN, COL_ERRONEOUS_DATA, COL_SHORT_STAY, COL_STANDART_STAY, COL_STANDART_EXTENDED_STAY, COL_LONG_STAY)

    stateDF
  }

  def mapToHotelState(row: Row): HotelState = {
    val hotelId = row(0).toString
    val withChildren = row(1).toString.toBoolean

    val hotelSateKey = HotelStateKey(hotelId, withChildren)

    val erroneousData = row(2).toString.toInt
    val shortStay = row(3).toString.toInt
    val standartStay = row(4).toString.toInt
    val standartExtendedStay = row(5).toString.toInt
    val longStay = row(6).toString.toInt

    val hotelState = HotelState(hotelSateKey, erroneousData, shortStay, standartStay,
      standartExtendedStay, longStay, EMPTY_RESULT_TYPE)
    hotelState
  }

  def aggregateHotelState(hotelStateKey: HotelStateKey,
                          hotelStates: Iterator[HotelState],
                          state: GroupState[HotelState]): Iterator[HotelState] = {
    if (state.hasTimedOut) {
      state.remove()
      Iterator(state.get)
    } else {
      val initialHotelState = initialHotelStateMap.
        getOrElse(hotelStateKey, HotelState(hotelStateKey, 0, 0, 0, 0, 0, EMPTY_RESULT_TYPE))

      val currentState = state.getOption
      val incrementedHotel = currentState
        .fold(incrementHotelStateCounts(initialHotelState, hotelStates))(
          currentHotelStates => incrementHotelStateCounts(currentHotelStates, hotelStates))

      state.update(incrementedHotel)
      Iterator(state.get)
    }
  }

  def incrementHotelStateCounts(initialHS: HotelState, newHotelStates: Iterator[HotelState]): HotelState = {
    newHotelStates.foreach(hotelState => {
      initialHS.erroneousData = initialHS.erroneousData + hotelState.erroneousData
      initialHS.shortStay = initialHS.shortStay + hotelState.shortStay
      initialHS.standartStay = initialHS.standartStay + hotelState.standartStay
      initialHS.standartExtendedStay = initialHS.standartExtendedStay + hotelState.standartExtendedStay
      initialHS.longStay = initialHS.longStay + hotelState.longStay
    })
    initialHS
  }

  def populateResultType(hs: HotelState): Unit = {
    val indexOfMaxType = List(hs.erroneousData, hs.shortStay, hs.standartStay, hs.standartExtendedStay, hs.longStay)
      .zipWithIndex.maxBy(_._1)._2

    hs.resultingType = STAY_TYPES(indexOfMaxType)
  }
}