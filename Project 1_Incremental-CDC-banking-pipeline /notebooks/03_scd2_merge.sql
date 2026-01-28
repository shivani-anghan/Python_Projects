MERGE INTO transactions_history t
USING cdc_changes s
ON t.transaction_id = s.transaction_id AND t.is_current = true

WHEN MATCHED THEN
  UPDATE SET
    t.is_current = false,
    t.effective_end_date = current_timestamp()

WHEN NOT MATCHED THEN
  INSERT (
    transaction_id,
    account_id,
    amount,
    status,
    transaction_date,
    effective_start_date,
    effective_end_date,
    is_current
  )
  VALUES (
    s.transaction_id,
    s.account_id,
    s.amount,
    s.status,
    s.transaction_date,
    current_timestamp(),
    NULL,
    true
  );
