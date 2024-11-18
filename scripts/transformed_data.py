from pyspark.sql import SparkSession
from pypspark.sql.functions import *
import os


# Spark Session

spark = SparkSession.builder \
        .appName("Retail Data Processing") \
        .config("spark.sql.warehouse.dir","/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

# Input and Output Directories
input_dir = "hdfs:///data/extracted/"
output_dir = "hdfs:///data/transformed/"

os.system(f"hdfs dfs -mkdir -p {output_dir}")


# Load extracted data

customers_df = spark.read.csv(f"{input_dir}/customers.csv", header=True,inferSchema=True)
orders_df = spark.read.csv(f"{input_dir}/orders.csv", header=True, inferSchema=True)


# Transformation

total_spending_df = orders_df.groupBy("customer_id") \
        .agg(sum(col("order_amount")).alias("total_spent"))


# Save transformed data

output_path  = os.path.join(output_dir,"total_spending.parquet")
total_spending_df.write.mode("overwrite").parquet(output_path)
print(f"Transformed data saved to {output_path})

spark.stop()
    
