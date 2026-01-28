def test_cdc_detects_changes():
    history = spark.table("transactions_history") \
        .filter("is_current = true")
    
    assert history.count() > 0
