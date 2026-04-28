## Medallion Architecture Assignment

**Name:** Abhishek Tiwari
**Session Date:** April 22, 2026

### Overview

- This project implements a simplified Medallion Architecture pipeline (Bronze → Silver → Gold) using PySpark and Delta Lake for e-commerce order data.

### What the Pipeline Does

* Bronze: Ingests raw data with metadata
* Silver: Deduplicates, validates, and separates bad records
* MERGE: Performs incremental upsert (update + insert)
* Gold: Creates business-level aggregations

### How to Run

1. Open Databricks Community Edition
2. Create a new notebook
3. Paste `medallion_pipeline.py` code
4. Click on **run cell** button

### Output Tables

* bronze_orders
* silver_orders
* silver_orders_quarantine
* gold_daily_revenue
* gold_customer_spend
