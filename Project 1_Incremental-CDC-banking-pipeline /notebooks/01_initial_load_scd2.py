from pyspark.sql.functions import *
from pyspark.sql.types import *

# Load Day 1 data
df = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv("/FileStore/transactions_day1.csv")

# Add SCD2 columns
scd_df = df \
    .withColumn("effective_start_date", current_timestamp()) \
    .withColumn("effective_end_date", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True))

# Write Delta table
scd_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("transactions_history")
