set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;

CREATE DATABASE IF NOT EXISTS hwxdemo;
USE hwxdemo;

drop table if exists flights_csv purge;
drop table if exists airports_csv purge;
drop table if exists airlines_csv purge;
drop table if exists planes_csv purge;

create table airports_csv(
iata string,
airport string,
city string,
state string,
country string,
lat double,
lon double
)
STORED AS TEXTFILE
LOCATION 's3a://hw-sampledata/airline_csv/airline_ontime.db/airports'
;

create table airlines_csv (
code string,
description string
)
STORED AS TEXTFILE
LOCATION 's3a://hw-sampledata/airline_csv/airline_ontime.db/airlines'
;


create table planes_csv (
tailnum string,
owner_type string,
manufacturer string,
issue_date string,
model string,
status string,
aircraft_type string,
engine_type string,
year int
)
STORED AS TEXTFILE
LOCATION 's3a://hw-sampledata/airline_csv/airline_ontime.db/planes'
;



create table flights_csv (
  DateOfFlight date,
  DepTime  int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier string,
  FlightNum int,
  TailNum string,
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Dest string,
  Distance int,
  TaxiIn int,
  TaxiOut int,
  Cancelled int,
  CancellationCode varchar(1),
  Diverted varchar(1),
  CarrierDelay int,
  WeatherDelay int,
  NASDelay int,
  SecurityDelay int,
  LateAircraftDelay int
) 
PARTITIONED BY (Year int)
STORED AS TEXTFILE
LOCATION 's3a://hw-sampledata/airline_csv/airline_ontime.db/flights'
TBLPROPERTIES("orc.bloom.filter.columns"="*")
;

