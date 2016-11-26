set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;
set hive.tez.container.size=2048;

drop table if exists flights purge;
drop table if exists airports purge;
drop table if exists airlines purge;
drop table if exists planes purge;


create table airports(
iata string,
airport string,
city string,
state string,
country string,
lat double,
lon double
)
STORED AS ORC
LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/airports'
;



create table airlines (
code string,
description string
)
STORED AS ORC
LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/airlines'
;

create table planes (
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
STORED AS ORC
LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/planes'
;

create table flights (
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime  int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier varchar(6),
  FlightNum int,
  TailNum varchar(8),
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin varchar(3),
  Dest varchar(3),
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
CLUSTERED BY (Month)
SORTED BY (DayofMonth) into 12 buckets
STORED AS ORC
LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights'
TBLPROPERTIES("orc.bloom.filter.columns"="*")
;

ALTER TABLE flights ADD PARTITION (year=1987) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=1987/';
ALTER TABLE flights ADD PARTITION (year=1988) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=1988/';
ALTER TABLE flights ADD PARTITION (year=1989) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=1989/';
ALTER TABLE flights ADD PARTITION (year=1990) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=1990/';
ALTER TABLE flights ADD PARTITION (year=1991) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=1991/';
ALTER TABLE flights ADD PARTITION (year=1992) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=1992/';
ALTER TABLE flights ADD PARTITION (year=1993) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=1993/';
ALTER TABLE flights ADD PARTITION (year=1994) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=1994/';
ALTER TABLE flights ADD PARTITION (year=1995) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=1995/';
ALTER TABLE flights ADD PARTITION (year=1996) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=1996/';
ALTER TABLE flights ADD PARTITION (year=1997) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=1997/';
ALTER TABLE flights ADD PARTITION (year=1998) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=1998/';
ALTER TABLE flights ADD PARTITION (year=1999) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=1999/';
ALTER TABLE flights ADD PARTITION (year=2000) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=2000/';
ALTER TABLE flights ADD PARTITION (year=2001) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=2001/';
ALTER TABLE flights ADD PARTITION (year=2002) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=2002/';
ALTER TABLE flights ADD PARTITION (year=2003) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=2003/';
ALTER TABLE flights ADD PARTITION (year=2004) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=2004/';
ALTER TABLE flights ADD PARTITION (year=2005) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=2005/';
ALTER TABLE flights ADD PARTITION (year=2006) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=2006/';
ALTER TABLE flights ADD PARTITION (year=2007) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=2007/';
ALTER TABLE flights ADD PARTITION (year=2008) LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/year=2008/';


ANALYZE TABLE flights COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE airports COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE airlines COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE planes COMPUTE STATISTICS FOR COLUMNS;


ANALYZE TABLE flights partition (year=1987) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=1988) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=1989) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=1990) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=1991) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=1992) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=1993) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=1994) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=1995) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=1996) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=1997) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=1998) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=1999) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=2000) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=2001) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=2002) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=2003) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=2004) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=2005) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=2006) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=2007) COMPUTE STATISTICS;
ANALYZE TABLE flights partition (year=2008) COMPUTE STATISTICS;
ANALYZE TABLE airports COMPUTE STATISTICS;
ANALYZE TABLE airlines COMPUTE STATISTICS;
ANALYZE TABLE planes COMPUTE STATISTICS;