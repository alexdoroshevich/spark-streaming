package com.epam.spark.streamig.homework

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }
}
