package com.epam.spark.streamig.homework.domain

case class HotelState(var hotelStateKey: HotelStateKey,
                      var erroneousData: Int,
                      var shortStay: Int,
                      var standartStay: Int,
                      var standartExtendedStay: Int,
                      var longStay: Int,
                      var resultingType: String)
