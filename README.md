1. hadoop fs -rm -r /homework/spark/streaming/checkpoint  
2. hadoop fs -rm -r /homework/spark/streaming/result
3. INSERT INTO TABLE hive_hotel_weather_topic SELECT `wthr_lng`, `wthr_lat`, `avg_tmpr_f`, `avg_tmpr_c`, `wthr_date`, `wthr_geo_hash`, `hotel_id`, `hotel_name`, `hotel_country`
   , `hotel_city`, `hotel_addess`, `hotel_lat`, `hotel_lng`, `hotel_geo_hash`, null AS `__key`, null AS `__partition`, -1 AS `__offset`, CURRENT_TIMESTAMP AS `__timestamp` FROM weather_hotel;