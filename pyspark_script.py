from pyspark.sql import SparkSession
from pypspark.sql.functions import *

spark = SparkSession.builder \
        .appName("Retail Data Processing") \
        .config("spark.sql.warehouse.dir","/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()


jdbc_url = "jdbc:mysql://localhost:3306/retail_db"
connection_properties = {
        "user" = "root",
        "password" = "Shadab",
        "driver" = "com.mysql.cj.jdbc.Driver"


# Load data from mysql

customers_df = spark.read.jdbc(jdbc_url,"customers",properties=connection_properties)
orders_df = spark.read.jdbc(jdbc_url,"orders",properties=connection_properties)


# Total spending per customer

total_spending_df = orders_df.groupBy("customer_id") \
        .agg(sum(co("order_amount")).alias("total_spend"))


# Write back output to hive

total_spending_df.write.mode("overwrite").saveAsTable("hive_retail.total_spending")

spark.stop()
    
