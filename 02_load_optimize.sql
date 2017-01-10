CREATE DATABASE IF NOT EXISTS hwxdemo;
USE hwxdemo;

msck repair table flights;

alter table airports add constraint airports_c1 primary key (iata) disable novalidate;
alter table airlines add constraint airlines_c1 primary key (code) disable novalidate;
alter table planes add constraint planes_c1 primary key (tailnum) disable novalidate;

alter table flights add constraint flights_new_c1 foreign key (Origin) references airports(iata) disable novalidate rely;
alter table flights add constraint flights_new_c2 foreign key (Dest) references airports(iata) disable novalidate rely;
alter table flights add constraint flights_new_c3 foreign key (UniqueCarrier) references airlines(code) disable novalidate rely;
alter table flights add constraint flights_new_c4 foreign key (TailNum) references planes(TailNum) disable novalidate rely;

ANALYZE TABLE airports COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE airlines COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE planes COMPUTE STATISTICS FOR COLUMNS;

