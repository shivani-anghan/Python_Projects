from pyspark.sql.functions import *

df = spark.read.table("transactions_history")

# 1. Null Checks
null_checks = df.select(
    count(when(col("transaction_id").isNull(), 1)).alias("null_transaction_id"),
    count(when(col("account_id").isNull(), 1)).alias("null_account_id"),
    count(when(col("amount").isNull(), 1)).alias("null_amount")
)

# 2. Duplicate Check
duplicate_check = df.groupBy("transaction_id", "effective_start_date") \
    .count().filter("count > 1")

# 3. Business Rule Validation
business_rules = df.filter("amount <= 0")

# 4. Write audit results
audit_results = (
    null_checks
    .withColumn("duplicate_records", lit(duplicate_check.count()))
    .withColumn("invalid_amount_records", lit(business_rules.count()))
    .withColumn("audit_timestamp", current_timestamp())
)

audit_results.write.format("delta") \
    .mode("append") \
    .saveAsTable("transactions_audit_log")
