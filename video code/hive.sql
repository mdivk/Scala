--Hive 

-- text format 
create database paslechoix_retail_db_txt;
use paslechoix_retail_db_txt;

create table orders (
  order_id int,
  order_date string,
  order_customer_id int,
  order_status string
) row format delimited fields terminated by ','
stored as textfile;

load data local inpath '/data/retail_db/orders' into table orders;

create table order_items (
  order_item_id int,
  order_item_order_id int,
  order_item_product_id int,
  order_item_quantity int,
  order_item_subtotal float,
  order_item_product_price float
) row format delimited fields terminated by ','
stored as textfile;

load data local inpath '/data/retail_db/order_items' into table order_items;

--orc format 
create database paslechoix_retail_db_orc;
use paslechoix_retail_db_orc;

create table orders (
  order_id int,
  order_date string,
  order_customer_id int,
  order_status string
) row format delimited fields terminated by ','
stored as orc;

load data local inpath '/data/retail_db/orders' into table orders;

create table order_items (
  order_item_id int,
  order_item_order_id int,
  order_item_product_id int,
  order_item_quantity int,
  order_item_subtotal float,
  order_item_product_price float
) row format delimited fields terminated by ','
stored as orc;

load data local inpath '/data/retail_db/order_items' into table order_items;

--
scala> sqlContext.sql("select * from orders limit 10").show
+--------+--------------------+-----------------+---------------+
|order_id|          order_date|order_customer_id|   order_status|
+--------+--------------------+-----------------+---------------+
|       1|2013-07-25 00:00:...|            11599|         CLOSED|
|       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|
|       3|2013-07-25 00:00:...|            12111|       COMPLETE|
|       4|2013-07-25 00:00:...|             8827|         CLOSED|
|       5|2013-07-25 00:00:...|            11318|       COMPLETE|
|       6|2013-07-25 00:00:...|             7130|       COMPLETE|
|       7|2013-07-25 00:00:...|             4530|       COMPLETE|
|       8|2013-07-25 00:00:...|             2911|     PROCESSING|
|       9|2013-07-25 00:00:...|             5657|PENDING_PAYMENT|
|      10|2013-07-25 00:00:...|             5648|PENDING_PAYMENT|
+--------+--------------------+-----------------+---------------+

