set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;

CREATE DATABASE IF NOT EXISTS hwxdemo;
USE hwxdemo;

drop table if exists flights_new purge;


create table flights_new (
  DateOfFlight date,
  UniqueCarrier string,
  FlightCount int
) 
CLUSTERED BY (DateOfFlight) INTO 6 BUCKETS
STORED AS ORC
TBLPROPERTIES("transactional"="true")
;

INSERT INTO TABLE flights_new
SELECT * FROM flights_agg;

SET hive.merge.cardinality.check=false;

explain 
MERGE INTO flights_new AS T USING flights_agg AS S
ON T.DateOfFlight = S.DateOfFlight AND S.UniqueCarrier = T.UniqueCarrier
WHEN MATCHED 
AND S.UniqueCarrier = 'AA' AND S.Year>=2004
THEN UPDATE
SET UniqueCarrier = 'UA';

MERGE INTO flights_new AS T USING flights_csv AS S
ON T.DateOfFlight = S.DateOfFlight AND T.DepTime = S.DepTime AND T.Origin = S.Origin
AND T.Dest=S.Dest AND S.FlightNum = T.FlightNum
WHEN NOT MATCHED 
AND S.UniqueCarrier = 'AA' AND Year>=2004
THEN INSERT VALUES
(s.DateOfFlight,
  s.DepTime,
  s.CRSDepTime,
  s.ArrTime,
  s.CRSArrTime,
  'UA',
  s.FlightNum,
  s.TailNum,
  s.ActualElapsedTime,
  s.CRSElapsedTime,
  s.AirTime,
  s.ArrDelay,
  s.DepDelay,
  s.Origin,
  s.Dest,
  s.Distance,
  s.TaxiIn,
  s.TaxiOut,
  s.Cancelled,
  s.CancellationCode,
  s.Diverted,
  s.CarrierDelay,
  s.WeatherDelay,
  s.NASDelay,
  s.SecurityDelay,
  s.LateAircraftDelay)
WHEN NOT MATCHED 
  THEN INSERT VALUES
(s.DateOfFlight,
  s.DepTime,
  s.CRSDepTime,
  s.ArrTime,
  s.CRSArrTime,
  s.UniqueCarrier,
  s.FlightNum,
  s.TailNum,
  s.ActualElapsedTime,
  s.CRSElapsedTime,
  s.AirTime,
  s.ArrDelay,
  s.DepDelay,
  s.Origin,
  s.Dest,
  s.Distance,
  s.TaxiIn,
  s.TaxiOut,
  s.Cancelled,
  s.CancellationCode,
  s.Diverted,
  s.CarrierDelay,
  s.WeatherDelay,
  s.NASDelay,
  s.SecurityDelay,
  s.LateAircraftDelay);