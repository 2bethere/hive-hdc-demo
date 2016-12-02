set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;

drop table if exists flights_hdfs purge;
drop table if exists airports_hdfs purge;
drop table if exists airlines_hdfs purge;
drop table if exists planes_hdfs purge;

create table airports_hdfs
STORED AS ORC
LOCATION '/tmp/airline_orc/airline_ontime.db/airports'
AS SELECT * from airports
;
create table airlines_hdfs
STORED AS ORC
LOCATION '/tmp/airline_orc/airline_ontime.db/airlines'
AS SELECT * from airlines
;


create table planes_hdfs 
STORED AS ORC
LOCATION '/tmp/airline_orc/airline_ontime.db/planes'
AS SELECT * from planes
;


create table flights_hdfs
STORED AS ORC
LOCATION '/tmp/airline_orc/airline_ontime.db/flights'
AS SELECT * from flights
;



alter table airports_hdfs add constraint airports_c1 primary key (iata) disable novalidate;
alter table airlines_hdfs add constraint airlines_c1 primary key (code) disable novalidate;
alter table planes_hdfs add constraint planes_c1 primary key (tailnum) disable novalidate;

alter table flights add constraint flights_new_c1 foreign key (Origin) references airports_hdfs(iata) disable novalidate rely;
alter table flights add constraint flights_new_c2 foreign key (Dest) references airports_hdfs(iata) disable novalidate rely;
alter table flights add constraint flights_new_c3 foreign key (UniqueCarrier) references airlines_hdfs(code) disable novalidate rely;
alter table flights add constraint flights_new_c4 foreign key (TailNum) references planes_hdfs(TailNum) disable novalidate rely;

ANALYZE TABLE airports_hdfs COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE airlines_hdfs COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE planes_hdfs COMPUTE STATISTICS FOR COLUMNS;

