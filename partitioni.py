#!/usr/bin/python
import sys
from datetime import timedelta, date
from pyhive import hive
from TCLIService.ttypes import TOperationState

if len(sys.argv) < 2:
    print "Run python partitioni.py host_name"
    exit()

def daterange(_start_date, _end_date):
    """ Return range of dates"""
    for day in range(int((_end_date - _start_date).days)):
        yield _start_date + timedelta(day)


host_name = sys.argv[1]
cursor = hive.connect(host_name,port=10500).cursor()
start_date = date(2001, 9, 12)
end_date = date(2008, 12, 31)
i = 1
for single_date in daterange(start_date, end_date):
    datestr = single_date.strftime("%Y-%m-%d")
    query_str = "ALTER TABLE flights ADD PARTITION (dateofflight=\'"+datestr+"\') LOCATION 's3a://hw-sampledata/airline_orc/airline_ontime.db/flights/dateofflight="+datestr+"/'"
    cursor.execute(query_str)
cursor.close()


