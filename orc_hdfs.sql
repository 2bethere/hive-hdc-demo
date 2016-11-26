set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;
set hive.tez.container.size=2048;

drop table if exists flights_hdfs purge;
drop table if exists airports_hdfs purge;
drop table if exists airlines_hdfs purge;
drop table if exists planes_hdfs purge;


create table airports_hdfs(
iata string,
airport string,
city string,
state string,
country string,
lat double,
lon double
)
STORED AS ORC
LOCATION '/data/airline_orc/airline_ontime.db/airports'
;


create table airlines_hdfs (
code string,
description string
)
STORED AS ORC
LOCATION '/data/airline_orc/airline_ontime.db/airlines'
;

create table planes_hdfs (
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
LOCATION '/data/airline_orc/airline_ontime.db/planes'
;

create table flights_hdfs (
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
LOCATION '/data/airline_orc/airline_ontime.db/flights'
TBLPROPERTIES("orc.bloom.filter.columns"="*")
;

ALTER TABLE flights_hdfs ADD PARTITION (year=1987) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=1987/';
ALTER TABLE flights_hdfs ADD PARTITION (year=1988) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=1988/';
ALTER TABLE flights_hdfs ADD PARTITION (year=1989) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=1989/';
ALTER TABLE flights_hdfs ADD PARTITION (year=1990) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=1990/';
ALTER TABLE flights_hdfs ADD PARTITION (year=1991) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=1991/';
ALTER TABLE flights_hdfs ADD PARTITION (year=1992) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=1992/';
ALTER TABLE flights_hdfs ADD PARTITION (year=1993) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=1993/';
ALTER TABLE flights_hdfs ADD PARTITION (year=1994) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=1994/';
ALTER TABLE flights_hdfs ADD PARTITION (year=1995) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=1995/';
ALTER TABLE flights_hdfs ADD PARTITION (year=1996) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=1996/';
ALTER TABLE flights_hdfs ADD PARTITION (year=1997) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=1997/';
ALTER TABLE flights_hdfs ADD PARTITION (year=1998) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=1998/';
ALTER TABLE flights_hdfs ADD PARTITION (year=1999) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=1999/';
ALTER TABLE flights_hdfs ADD PARTITION (year=2000) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=2000/';
ALTER TABLE flights_hdfs ADD PARTITION (year=2001) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=2001/';
ALTER TABLE flights_hdfs ADD PARTITION (year=2002) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=2002/';
ALTER TABLE flights_hdfs ADD PARTITION (year=2003) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=2003/';
ALTER TABLE flights_hdfs ADD PARTITION (year=2004) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=2004/';
ALTER TABLE flights_hdfs ADD PARTITION (year=2005) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=2005/';
ALTER TABLE flights_hdfs ADD PARTITION (year=2006) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=2006/';
ALTER TABLE flights_hdfs ADD PARTITION (year=2007) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=2007/';
ALTER TABLE flights_hdfs ADD PARTITION (year=2008) LOCATION '/data/airline_orc/airline_ontime.db/flights/year=2008/';


ANALYZE TABLE flights_hdfs partition (year=1987) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=1988) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=1989) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=1990) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=1991) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=1992) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=1993) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=1994) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=1995) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=1996) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=1997) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=1998) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=1999) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=2000) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=2001) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=2002) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=2003) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=2004) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=2005) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=2006) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=2007) COMPUTE STATISTICS;
ANALYZE TABLE flights_hdfs partition (year=2008) COMPUTE STATISTICS;
ANALYZE TABLE airports_hdfs COMPUTE STATISTICS;
ANALYZE TABLE airlines_hdfs COMPUTE STATISTICS;
ANALYZE TABLE planes_hdfs COMPUTE STATISTICS;

ANALYZE TABLE flights_hdfs COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE airports_hdfs COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE airlines_hdfs COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE planes_hdfs COMPUTE STATISTICS FOR COLUMNS;


create table flights_new (
  Year int,
  Month int,
  DayOfMonth int,
  DayOfWeek int,
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
PARTITIONED BY (DateOfFlight date)
STORED AS ORC
LOCATION '/data/airline_orc/airline_ontime.db/flights_new'
TBLPROPERTIES("orc.bloom.filter.columns"="*")
;

set hive.exec.max.dynamic.partitions=10000;

INSERT INTO TABLE flights_new
PARTITION(DateOfFlight)
SELECT Year,
  Month,
  DayOfMonth,
  DayOfWeek,
  DepTime,
  CRSDepTime,
  ArrTime,
  CRSArrTime ,
  cast(UniqueCarrier as string) as UniqueCarrier,
  FlightNum ,
  cast(TailNum as string) as TailNum,
  ActualElapsedTime ,
  CRSElapsedTime ,
  AirTime ,
  ArrDelay ,
  DepDelay ,
  cast(Origin as string) as Origin,
  cast(Dest as string) as Dest,
  Distance ,
  TaxiIn ,
  TaxiOut ,
  Cancelled ,
  CancellationCode ,
  Diverted ,
  CarrierDelay ,
  WeatherDelay ,
  NASDelay ,
  SecurityDelay ,
  LateAircraftDelay,
  cast(concat(concat(concat(cast(year as string),"-"),concat(cast(month as string),"-")),cast(DayofMonth as string)) as date) as DateOfFlight
FROM flights_hdfs;


select cast(concat(concat(concat(cast(year as string),"-"),concat(cast(month as string),"-")),cast(DayofMonth as string)) as date) from flights limit 1;

alter table flights_new add constraint flights_new_c1 foreign key (Origin) references airports_hdfs(iata) disable novalidate rely;
alter table flights_new add constraint flights_new_c2 foreign key (Dest) references airports_hdfs(iata) disable novalidate rely;
alter table flights_new add constraint flights_new_c3 foreign key (UniqueCarrier) references airlines_hdfs(code) disable novalidate rely;
alter table flights_new add constraint flights_new_c4 foreign key (TailNum) references planes_hdfs(TailNum) disable novalidate rely;


create table flights_new_mpart (
  DateOfFlight date,
  Month int,
  DayOfMonth int,
  DayOfWeek int,
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
PARTITIONED BY (year int)
CLUSTERED BY (Month)
STORED AS ORC
LOCATION '/data/airline_orc/airline_ontime.db/flights_new_mpart'
TBLPROPERTIES("orc.bloom.filter.columns"="*")
;