from pyspark.sql.functions import *

# Load current active records
target_df = spark.table("transactions_history") \
    .filter("is_current = true")

# Load CDC data
source_df = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv("/FileStore/transactions_day2.csv")

# Detect changes
cdc_df = source_df.alias("s") \
    .join(target_df.alias("t"), "transaction_id") \
    .filter("""
        s.amount <> t.amount OR
        s.status <> t.status
    """) \
    .select("s.*")

cdc_df.createOrReplaceTempView("cdc_changes")
