# Incremental CDC Pipeline for Banking Transactions

## Project Overview

This project demonstrates a production-style incremental Change Data Capture (CDC) pipeline for banking transaction data. It uses SCD Type 2 to maintain a full historical audit trail while avoiding expensive full reloads. The pipeline is designed to support regulatory compliance, analytics, and reporting use cases.

## 🏦 Business Problem

Banking systems generate thousands of transaction updates daily. Performing full data reloads is inefficient, costly, and not audit-friendly.
This solution captures only new and changed records, preserves historical versions, and enables time-travel analysis.

## 🧱 Architecture
Source Files → CDC Detection → Delta MERGE (SCD Type 2)
             → Data Quality Checks
             → Audit Logging
             → Curated History Tables

## Tech Stack

- Python / PySpark – ETL and transformations

- Delta Lake – SCD Type 2 storage, MERGE, time travel

- Spark SQL – CDC logic and merge queries

- Databricks Community Edition – Pipeline execution

- GitHub Actions – CI/CD simulation (pipeline validation)

## Key Features

- Incremental CDC processing

- SCD Type 2 historical tracking

- Delta MERGE INTO operations

- Data quality validations (nulls, duplicates, business rules)

- Audit logging for compliance and traceability

- Time travel and rollback using Delta Lake

## 📊 Results & Outcomes

- Reduced processing time and compute cost

- Enabled full audit and compliance reporting

- Improved data reliability and historical traceability

- Analytics-ready curated tables for BI tools

## How to Run

- Upload source CSV files to Databricks File System (DBFS)

- Open the Databricks notebook

- Run cells sequentially to simulate daily CDC loads

- Query Delta tables to view historical changes