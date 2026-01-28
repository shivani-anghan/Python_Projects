from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

def test_no_null_transaction_id():
    df = spark.table("transactions_history")
    assert df.filter(col("transaction_id").isNull()).count() == 0

def test_positive_amount():
    df = spark.table("transactions_history")
    assert df.filter(col("amount") <= 0).count() == 0
