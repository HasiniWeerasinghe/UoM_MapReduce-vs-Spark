CREATE EXTERNAL TABLE flight_delays (RowNum int,Year int,Month int,DayofMonth int,DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,CRSArrTime int,UniqueCarriert string,FlightNum int,TailNum string,ActualElapsedTime int,CRSElapsedTime int,AirTime int,ArrDelay int,DepDelay int,Origin string,Dest string,Distance int,TaxiIn int,TaxiOut int,Cancelled int,CancellationCode string,Diverted int,CarrierDelay int,WeatherDelay int,NASDelay int,SecurityDelay int,LateAircraftDelay int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION "s3://hivevsspark/input/";


SELECT Year, avg((CarrierDelay /ArrDelay)*100) from flight_delay GROUP BY Year;


SELECT Year, avg((NASDelay /ArrDelay)*100) from flight_delay GROUP BY Year;


SELECT Year, avg((WeatherDelay /ArrDelay)*100) from flight_delay GROUP BY Year;


SELECT Year, avg((LateAircraftDelay /ArrDelay)*100) from flight_delay GROUP BY Year;


SELECT Year, avg((SecurityDelay /ArrDelay)*100) from flight_delay GROUP BY Year;
