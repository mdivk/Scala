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

Union: duplicates will be eliminated
Union All: duplicates will be retained

Analytical functions:
OVER
RANK
ROW_NUMBER


select o.order_id, o.order_date, o.order_status, round(sum(oi.order_item_subtotal),2) order_revenue
from orders o join order_items oi on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_id, o.order_date, o.order_status
having sum(oi.order_item_subtotal) >= 1000
order by o.order_date, order_revenue desc;

orders:
order_id                int
order_date              string
order_customer_id       int
order_status            string

hive (paslechoix)> select * from orders limit 10;
OK
1       2013-07-25 00:00:00.0   11599   CLOSED
2       2013-07-25 00:00:00.0   256     PENDING_PAYMENT
3       2013-07-25 00:00:00.0   12111   COMPLETE
4       2013-07-25 00:00:00.0   8827    CLOSED
5       2013-07-25 00:00:00.0   11318   COMPLETE
6       2013-07-25 00:00:00.0   7130    COMPLETE
7       2013-07-25 00:00:00.0   4530    COMPLETE
8       2013-07-25 00:00:00.0   2911    PROCESSING
9       2013-07-25 00:00:00.0   5657    PENDING_PAYMENT
10      2013-07-25 00:00:00.0   5648    PENDING_PAYMENT

order_items:

order_item_id           int
order_item_order_id     int
order_item_product_id   int
order_item_quantity     tinyint
order_item_subtotal     double
order_item_product_price        double

hive (paslechoix)> select * from order_items limit 10;
1       1       957     1       299.98  299.98
2       2       1073    1       199.99  199.99
3       2       502     5       250.0   50.0
4       2       403     1       129.99  129.99
5       4       897     2       49.98   24.99
6       4       365     5       299.95  59.99
7       4       502     3       150.0   50.0
8       4       1014    4       199.92  49.98
9       5       957     1       299.98  299.98
10      5       365     5       299.95  59.99


select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal, 
round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) order_revenue,
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) pct_revenue,
round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')

select * from (
select o.order_id, date_format(o.order_date, 'YYYYMMDD') order_date, o.order_status, oi.order_item_subtotal, 
round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) order_revenue,
round(oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2),2) pct_revenue,
round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')) q
where order_revenue >= 1000 and order_id = 57779
order by order_date, order_revenue desc limit 20;

order_id order_date  order_status	 order_item_subtotal order_revenue pct_revenue avg_revenue
57779    2013-07-25  COMPLETE        149.94  			 1649.8        0.09		   329.96
57779    2013-07-25  COMPLETE        399.98  			 1649.8        0.24		   329.96
57779    2013-07-25  COMPLETE        299.98  			 1649.8        0.18		   329.96
57779    2013-07-25  COMPLETE        499.95  			 1649.8        0.30		   329.96
57779    2013-07-25  COMPLETE        299.95  			 1649.8        0.18		   329.96
12       2013-07-25  CLOSED  		 149.94  			 1299.87       0.11		   259.97
12       2013-07-25  CLOSED  		 299.98  			 1299.87       0.23		   259.97
12       2013-07-25  CLOSED  		 250.0   			 1299.87       0.19		   259.97
12       2013-07-25  CLOSED  		 100.0   			 1299.87       0.07		   259.97
12       2013-07-25  CLOSED  		 499.95  			 1299.87       0.38		   259.97
28       2013-07-25  COMPLETE        299.98  			 1159.9        0.25		   231.98
28       2013-07-25  COMPLETE        59.99   			 1159.9        0.05		   231.98
28       2013-07-25  COMPLETE        99.99   			 1159.9        0.08		   231.98
28       2013-07-25  COMPLETE        299.98  			 1159.9        0.25		   231.98
28       2013-07-25  COMPLETE        399.96  			 1159.9        0.34		   231.98
62       2013-07-25  CLOSED  		 299.98  			 1149.94       0.26		   287.49
62       2013-07-25  CLOSED  		 399.98  			 1149.94       0.34		   287.49
62       2013-07-25  CLOSED  		 50.0    			 1149.94       0.04		   287.49
62       2013-07-25  CLOSED  		 399.98  			 1149.94       0.34		   287.49
57764    2013-07-25  COMPLETE        199.99  			 1149.92       0.17		   287.48



From MySQL:

select o.order_id, date_format(o.order_date, '%Y-%m-%d') order_date, o.order_status, oi.order_item_subtotal
from order_items oi
inner join orders o on o.order_id = oi.order_item_order_id
where o.order_id = 57779;
+----------+--------------+--------------+---------------------+
| order_id | order_date   | order_status | order_item_subtotal |
+----------+--------------+--------------+---------------------+
|    57779 | 2013-07-25   | COMPLETE     |              299.98 |
|    57779 | 2013-07-25   | COMPLETE     |              399.98 |
|    57779 | 2013-07-25   | COMPLETE     |              299.95 |
|    57779 | 2013-07-25   | COMPLETE     |              149.94 |
|    57779 | 2013-07-25   | COMPLETE     |              499.95 |
+----------+--------------+--------------+---------------------+

select o.order_id, date_format(o.order_date, '%Y-%m-%d') order_date, o.order_status, 
sum(oi.order_item_subtotal) order_revenue,
oi.order_item_subtotal/sum(oi.order_item_subtotal) pct_revenue,
sum(oi.order_item_subtotal) / count(1) avg_revenue
from order_items oi
inner join orders o on o.order_id = oi.order_item_order_id
where o.order_id = 57779
group by order_id, order_item_subtotal;

+----------+------------+--------------+--------------------+-------------+--------------------+
| order_id | order_date | order_status | order_revenue      | pct_revenue | avg_revenue        |
+----------+------------+--------------+--------------------+-------------+--------------------+
|    57779 | 2013-07-25 | COMPLETE     | 149.94000244140625 |           1 | 149.94000244140625 |
|    57779 | 2013-07-25 | COMPLETE     | 299.95001220703125 |           1 | 299.95001220703125 |
|    57779 | 2013-07-25 | COMPLETE     |  299.9800109863281 |           1 |  299.9800109863281 |
|    57779 | 2013-07-25 | COMPLETE     |  399.9800109863281 |           1 |  399.9800109863281 |
|    57779 | 2013-07-25 | COMPLETE     | 499.95001220703125 |           1 | 499.95001220703125 |
+----------+------------+--------------+--------------------+-------------+--------------------+

select o.order_id, date_format(o.order_date, '%Y-%m-%d') order_date, o.order_status, 
sum(oi.order_item_subtotal) order_revenue,
oi.order_item_subtotal/sum(oi.order_item_subtotal) pct_revenue,
sum(oi.order_item_subtotal) / count(1) avg_revenue
from order_items oi
inner join orders o on o.order_id = oi.order_item_order_id
where o.order_id = 57779
group by order_id, order_item_subtotal;




--Ranking using Analytical functions
select * from (
select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal, 
round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) order_revenue,
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) pct_revenue,
round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue,
rank() over (partition by o.order_id order by oi.order_item_subtotal desc) rnk_revenue,
dense_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) dense_rnk_revenue,
percent_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) pct_rnk_revenue,
row_number() over (partition by o.order_id order by oi.order_item_subtotal desc) rn_orderby_revenue,
row_number() over (partition by o.order_id) rn_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')) q
where order_revenue >= 1000 and order_id in (57779, 12, 28, 62)
order by order_date, order_revenue desc, rnk_revenue;

order_id order_date  order_status	 order_item_subtotal order_revenue pct_revenue avg_revenue rnk_revenue dense_rnk_revenue pct_rnk_revenue rn_orderby_revenue rn_revenue
57779   2013-07-25   COMPLETE        499.95  			 1649.8  		0.30     	329.96  	1       	1       		0.0     			1       			5
57779   2013-07-25   COMPLETE        399.98  			 1649.8  		0.24     	329.96  	2       	2       		0.25    			2       			3
57779   2013-07-25   COMPLETE        299.98  			 1649.8  		0.18     	329.96  	3       	3       		0.5     			3       			2
57779   2013-07-25   COMPLETE        299.95  			 1649.8  		0.18     	329.96  	4       	4       		0.75    			4       			4
57779   2013-07-25   COMPLETE        149.94  			 1649.8  		0.09     	329.96  	5       	5       		1.0     			5       			1
12      2013-07-25   CLOSED  		 499.95  			 1299.87 		0.38     	259.97  	1       	1       		0.0     			1       			4
12      2013-07-25   CLOSED  		 299.98  			 1299.87 		0.23     	259.97  	2       	2       		0.25    			2       			1
12      2013-07-25   CLOSED  		 250.0   			 1299.87 		0.19     	259.97  	3       	3       		0.5     			3       			5
12      2013-07-25   CLOSED  		 149.94  			 1299.87 		0.11     	259.97  	4       	4       		0.75    			4       			3
12      2013-07-25   CLOSED  		 100.0   			 1299.87 		0.07     	259.97  	5       	5       		1.0     			5       			2
28      2013-07-25   COMPLETE        399.96  			 1159.9  		0.34     	231.98  	1       	1       		0.0     			1       			2
28      2013-07-25   COMPLETE        299.98  			 1159.9  		0.25     	231.98  	2       	2       		0.25    			3       			3
28      2013-07-25   COMPLETE        299.98  			 1159.9  		0.25     	231.98  	2       	2       		0.25    			2       			1
28      2013-07-25   COMPLETE        99.99   			 1159.9  		0.08     	231.98  	4       	3       		0.75    			4       			4
28      2013-07-25   COMPLETE        59.99   			 1159.9  		0.05     	231.98  	5       	4       		1.0     			5       			5
62      2013-07-25   CLOSED  		 399.98  			 1149.94 		0.34     	287.49  	1       	1       		0.0     			1       			1
62      2013-07-25   CLOSED  		 399.98  			 1149.94 		0.34     	287.49  	1       	1       		0.0     			2       			2
62      2013-07-25   CLOSED  		 299.98  			 1149.94 		0.26     	287.49  	3       	2       		0.66    			3       			3
62      2013-07-25   CLOSED  		 50.0    			 1149.94 		0.04     	287.49  	4       	3       		1.0     			4       			4


select * from (
select o.order_id, date_format(o.order_date, 'YYYYMMDD') order_date, o.order_status, oi.order_item_subtotal, 
round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) order_revenue,
round(oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2),2) pct_revenue,
round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue,
rank() over (partition by o.order_id order by oi.order_item_subtotal desc) rnk_revenue,
dense_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) dense_rnk_revenue,
percent_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) pct_rnk_revenue,
row_number() over (partition by o.order_id order by oi.order_item_subtotal desc) rn_orderby_revenue,
row_number() over (partition by o.order_id) rn_revenue,
lead(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) lead_order_item_subtotal,
lag(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) lag_order_item_subtotal,
first_value(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) first_order_item_subtotal,
last_value(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) last_order_item_subtotal
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')) q
where order_revenue >= 1000 and order_id in (57779, 12, 28, 62)
order by order_date, order_revenue desc, rnk_revenue;
