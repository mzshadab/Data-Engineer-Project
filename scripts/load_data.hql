create database if not exists hive_retail;

use hive_retail;

create table if not exists total_spending(
customer_id int,
total_spent decimal(10,2)
)
stored as parquet;

load data inpath ''hdfs:///data/transformed/total_spending.parquet
overwrite into table total_spending;
