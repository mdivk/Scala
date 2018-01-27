--Sql functions
substr or substring
instr: return the index of the second str in the first string 
hive (paslechoix_retail_db_orc)> select instr("How are you", 'are');
OK
5

like
rlike: regex
length
lcase or lower
ucase or upper
trim, ltrim, rtrim
lpad,rpad: 
hive (paslechoix_retail_db_orc)> desc function lpad;
OK
lpad(str, len, pad) - Returns str, left-padded with pad to a length of len
cast
hive (paslechoix_retail_db_orc)> select order_date from orders limit 1;
OK
2013-07-25 00:00:00.0

select cast("12" as int)
hive (paslechoix_retail_db_orc)> select cast(substring(order_date, 6, 2) as int) from orders limit 1;
OK
7

select split('Hello World, How are you?', ' ');
OK
["Hello","World,","How","are","you?"]

select index(split('Hello World, How are you?', ' '), 4);
hive (paslechoix_retail_db_orc)> select index(split('Hello World, How are you?', ' '), 4);
OK
you?

--Important functions
current_date
current_timestamp
hive (paslechoix_retail_db_orc)> select current_date;
OK
2018-01-27
Time taken: 0.604 seconds, Fetched: 1 row(s)
hive (paslechoix_retail_db_orc)> select current_timestamp;
OK
2018-01-27 17:57:12.496

date_add
date_format
hive (paslechoix_retail_db_orc)> select date_format(current_date, 'y');
OK
2018

date_sub
datediff
day
dayofmonth

to_date
to_unix_timestamp
to_utc_timestamp
from_unixtime
from_utc_timestamp

select current_date;
OK
2018-01-27
select to_unix_timestamp(current_date);
OK
1517029200
select from_unixtime(1517029200);
OK
2018-01-27 00:00:00

minute
month
months_between
next_day

select order_status, count(1) as sub_total from orders group by order_status order by sub_total desc;
COMPLETE        22899
PENDING_PAYMENT 15030
PROCESSING      8275
PENDING 7610
CLOSED  7556
ON_HOLD 3798
SUSPECTED_FRAUD 1558
CANCELED        1428
PAYMENT_REVIEW  729
//Note: in HiveSQL, count(1) cannot be used in order by, it must be assigned to a alias and use the alias in the order by clause



