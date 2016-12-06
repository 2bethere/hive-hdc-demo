set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;

drop table if exists flights_hdfs purge;
drop table if exists airports_hdfs purge;
drop table if exists airlines_hdfs purge;
drop table if exists planes_hdfs purge;

create table airports_hdfs
STORED AS ORC
AS SELECT * from airports
;
create table airlines_hdfs
STORED AS ORC
AS SELECT * from airlines
;


create table planes_hdfs 
STORED AS ORC
AS SELECT * from planes
;



create table flights_hdfs (
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
STORED AS ORC
TBLPROPERTIES("orc.bloom.filter.columns"="*")
;
INSERT OVERWRITE TABLE flights_hdfs PARTITION(year) 
SELECT * FROM flights;



alter table airports_hdfs add constraint airports_hdfs_c1 primary key (iata) disable novalidate;
alter table airlines_hdfs add constraint airlines_hdfs_c1 primary key (code) disable novalidate;
alter table planes_hdfs add constraint planes_hdfs_c1 primary key (tailnum) disable novalidate;

alter table flights add constraint flights_hdfs_new_c1 foreign key (Origin) references airports_hdfs(iata) disable novalidate rely;
alter table flights add constraint flights_hdfs_new_c2 foreign key (Dest) references airports_hdfs(iata) disable novalidate rely;
alter table flights add constraint flights_hdfs_new_c3 foreign key (UniqueCarrier) references airlines_hdfs(code) disable novalidate rely;
alter table flights add constraint flights_hdfs_new_c4 foreign key (TailNum) references planes_hdfs(TailNum) disable novalidate rely;

ANALYZE TABLE airports_hdfs COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE airlines_hdfs COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE planes_hdfs COMPUTE STATISTICS FOR COLUMNS;

