CREATE DATABASE IF NOT EXISTS hwxdemo;
USE hwxdemo;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;

drop table if exists flights_csv purge;
drop table if exists airports_csv purge;
drop table if exists airlines_csv purge;
drop table if exists planes_csv purge;


create table airports_csv
STORED AS ORC
AS SELECT * from airports
;
create table airlines_csv
STORED AS ORC
AS SELECT * from airlines
;


create table planes_csv
STORED AS ORC
AS SELECT * from planes
;