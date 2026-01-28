# --- Load CSV files from Databricks Volume ---
df_day1 = spark.read.option("header", True).csv("/Volumes/workspace/default/cdc_pipeline_volume/transactions_day1.csv ")
df_day2 = spark.read.option("header", True).csv("/Volumes/workspace/default/cdc_pipeline_volume/transactions_day2.csv")

# Optional: preview first 5 rows
df_day1.show(5)
df_day2.show(5)

# =============================================
# Pipeline Orchestrator - Incremental CDC Banking
# Author: Your Name
# Purpose: End-to-end CDC + SCD2 + Quality + Performance
# =============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Incremental_CDC_Pipeline").getOrCreate()

# =============================================
# 1. Load CDC Data
# =============================================

# Set input file (simulate ADF parameter)
input_file = "/FileStore/transactions_day2.csv"

cdc_df = spark.read.option("header", True).option("inferSchema", True).csv(input_file)
print(f"CDC Input Rows: {cdc_df.count()}")
cdc_df.show()

# =============================================
# 2️. Load Current Active Transactions
# =============================================

target_df = spark.table("transactions_history").filter("is_current = true")

# =============================================
# 3️. Detect Changes for CDC (Upserts)
# =============================================

# Join CDC to current table to find updated/new records
cdc_changes = cdc_df.alias("s") \
    .join(target_df.alias("t"), "transaction_id", "left") \
    .filter(
        (col("t.transaction_id").isNull()) | # New Record
        (col("s.amount") != col("t.amount")) |
        (col("s.status") != col("t.status"))
    ) \
    .select("s.*")

print(f"Detected Changes: {cdc_changes.count()}")
cdc_changes.show()

# =============================================
# 4️. Apply SCD Type 2 Logic (Delta Merge)
# =============================================

# Existing active records that need to be closed
records_to_close = target_df.alias("t") \
    .join(cdc_changes.alias("s"), "transaction_id") \
    .filter(col("t.is_current") == True) \
    .select("t.*") \
    .withColumn("is_current", lit(False)) \
    .withColumn("effective_end_date", current_timestamp())

# Update current records to inactive
records_to_close.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("transactions_history")

# Insert new/updated records
new_records = cdc_changes.withColumn("effective_start_date", current_timestamp()) \
    .withColumn("effective_end_date", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True))

new_records.write.format("delta") \
    .mode("append") \
    .saveAsTable("transactions_history")

print("CDC Merge / SCD2 Applied Successfully")

# =============================================
# 5. Data Quality Checks / Audit Logging
# =============================================

df_history = spark.table("transactions_history")

# Null Checks
null_checks = df_history.select(
    count(when(col("transaction_id").isNull(), 1)).alias("null_transaction_id"),
    count(when(col("account_id").isNull(), 1)).alias("null_account_id"),
    count(when(col("amount").isNull(), 1)).alias("null_amount")
)

# Duplicate Check
duplicate_count = df_history.groupBy("transaction_id", "effective_start_date").count().filter("count > 1").count()

# Business Rule Validation
invalid_amount_count = df_history.filter("amount <= 0").count()

# Write audit log
audit_df = null_checks.withColumn("duplicate_records", lit(duplicate_count)) \
    .withColumn("invalid_amount_records", lit(invalid_amount_count)) \
    .withColumn("audit_timestamp", current_timestamp())

audit_df.write.format("delta").mode("append").saveAsTable("transactions_audit_log")

print("Data Quality Checks Completed")
audit_df.show()

# =============================================
# 6️. Performance Optimization
# =============================================

# Re-partition by transaction_date
df_history.write.format("delta") \
    .partitionBy("transaction_date") \
    .mode("overwrite") \
    .saveAsTable("transactions_history")

# Optimize + ZORDER for query performance
spark.sql("""
OPTIMIZE transactions_history
ZORDER BY (transaction_date, account_id)
""")

print("✅ Performance Optimization Applied (Partition + ZORDER)")

# =============================================
# 7️. Pipeline Complete
# =============================================
print("Pipeline Execution Completed Successfully!")
