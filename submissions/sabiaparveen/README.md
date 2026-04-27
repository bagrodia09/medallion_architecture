# Medallion Architecture Assignment

## Name
sabia parveen

## Session Date
27-Apr-2026

## Project Overview
This project implements a simplified Medallion Architecture pipeline using PySpark and Delta Lake.

The pipeline processes sample e-commerce order data through Bronze, Silver, and Gold layers. 
It includes data deduplication, validation, quarantine handling, incremental MERGE updates, and business aggregations.

## Pipeline Layers

### Bronze Layer
- Stores raw source data exactly as received
- Adds metadata columns:
    - _ingestion_timestamp
    - _source_system

### Silver Layer
- Removes duplicate records using order_id
- Converts data types
- Validates records
- Splits bad records into quarantine table

### Gold Layer
Creates reporting tables:

1. daily_revenue_by_region
    - Revenue by region and date

2. customer_spend
    - Total spend by each customer

## How to Run

1. Open Databricks Community Edition
2. Create a new notebook
3. Paste code from `medallion_pipeline.py`
4. Run all cells

## Design Decisions

- Used Delta Lake tables for storage
- Used MERGE for incremental upsert logic
- Added quarantine table for invalid records instead of deleting bad data
- Added comments and step headers for readability
- Included final verification counts

## Output Tables

- bronze_orders
- silver_orders
- quarantine_orders
- gold_daily_revenue
- gold_customer_spend