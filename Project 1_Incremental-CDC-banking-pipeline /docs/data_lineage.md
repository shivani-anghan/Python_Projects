# Data Lineage – Banking CDC Pipeline

## Source
- Daily transaction CSV files from banking systems

## Ingestion
- Files ingested into DBFS (Raw Zone)

## Processing
- CDC logic detects new and changed records
- SCD Type 2 logic applied using Delta MERGE

## Storage
- Delta Lake tables with historical tracking

## Consumption
- Audit queries
- Compliance reporting
- Time-travel & rollback

## Governance
- Data quality checks
- Audit logs
- Versioned Delta tables
