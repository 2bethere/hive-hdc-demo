set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;

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
LOCATION '/data/airline_orc/airline_ontime.db/flights'
TBLPROPERTIES("orc.bloom.filter.columns"="*")
;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;




alter table airports add constraint airports_c1 primary key (iata) disable novalidate;
alter table airlines add constraint airlines_c1 primary key (code) disable novalidate;
alter table planes add constraint planes_c1 primary key (tailnum) disable novalidate;

alter table flights_new add constraint flights_new_c1 foreign key (Origin) references airports(iata) disable novalidate rely;
alter table flights_new add constraint flights_new_c2 foreign key (Dest) references airports(iata) disable novalidate rely;
alter table flights_new add constraint flights_new_c3 foreign key (UniqueCarrier) references airlines(code) disable novalidate rely;
alter table flights_new add constraint flights_new_c4 foreign key (TailNum) references planes(TailNum) disable novalidate rely;

ANALYZE TABLE flights COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE airports COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE airlines COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE planes COMPUTE STATISTICS FOR COLUMNS;

