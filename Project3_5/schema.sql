-- Write down the SQL statements you wrote to createing optimized tables
-- and to populate those tables in this file.
-- Remember to add comments explaining why you did so.

CREATE EXTERNAL TABLE lineorder_opt(
	lo_orderkey INT,
	lo_linenumber INT,
	lo_custkey INT,
	lo_partkey INT,
	lo_suppkey INT,
	lo_orderdate INT,
	lo_orderpriority STRING,
	lo_shippriority STRING,
	lo_quantity INT,
	lo_extendedprice INT,
	lo_ordertotalprice INT,
	lo_discount INT,
	lo_revenue INT,
	lo_supplycost INT,
	lo_tax INT,
	lo_commitdate INT,
	lo_shipmode STRING)
partitioned by (years smallint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/data/lineorder_opt/';

create table lineorder_opt like lineorder STORED AS PARQUET;
set PARQUET_COMPRESSION_CODEC=snappy;
insert into lineorder_opt partition (years=1997) select * from lineorder where lo_orderdate between 19970101 and 19971231;
insert into lineorder_opt partition (years=1998) select * from lineorder where lo_orderdate not between 19970101 and 19971231;
insert into lineorder_opt select * from lineorder;

create table part_opt like part STORED AS PARQUET;
set PARQUET_COMPRESSION_CODEC=snappy;
insert into part_opt select * from part;

create table supplier_opt like supplier STORED AS PARQUET;
set PARQUET_COMPRESSION_CODEC=snappy;
insert into supplier_opt select * from supplier;

create table customer_opt like customer STORED AS PARQUET;
set PARQUET_COMPRESSION_CODEC=snappy;
insert into customer_opt select * from customer;

CREATE EXTERNAL TABLE dwdate_opt(
	d_datekey INT,
	d_date STRING,
	d_dayofweek STRING,
	d_month STRING,
	d_year INT,
	d_yearmonthnum INT,
	d_yearmonth STRING,
	d_daynuminweek INT,
	d_daynuminmonth INT,
	d_daynuminyear INT,
	d_monthnuminyear INT,
	d_weeknuminyear INT,
	d_sellingseason STRING,
	d_lastdayinweekfl STRING,
	d_lastdayinmonthfl STRING,
	d_holidayfl STRING,
	d_weekdayfl STRING)
partitioned by (year smallint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/data/dwdate_opt/';
set PARQUET_COMPRESSION_CODEC=snappy;
insert into dwdate_opt partition (year=1997) select * from dwdate where d_year=1997;
insert into dwdate_opt partition (year=1998) select * from dwdate where d_year!=1997;


