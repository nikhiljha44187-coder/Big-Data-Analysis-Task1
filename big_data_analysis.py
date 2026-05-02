# Big Data Analysis using PySpark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

# Create Spark Session
spark = SparkSession.builder.appName("Big Data Analysis").getOrCreate()

# Load Dataset
df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)

# Show Data
df.show()

# Data Cleaning
df = df.dropna()
df = df.dropDuplicates()

# Analysis 1: Total Sales by Product
product_sales = df.groupBy("Product").agg(sum("Sales").alias("Total_Sales"))
product_sales.show()

# Analysis 2: Average Sales by Region
region_avg = df.groupBy("Region").agg(avg("Sales").alias("Avg_Sales"))
region_avg.show()

# Analysis 3: Top 5 Products
top_products = product_sales.orderBy(col("Total_Sales").desc()).limit(5)
top_products.show()

# Total Orders
order_count = df.count()
print("Total Orders:", order_count)

# Stop Spark
spark.stop()
