# medallion_architecture

🏅 Medallion Architecture — CodeCamp Assignment
Session: Medallion Architecture: Building Scalable & Reliable Data Pipelines
Presenter: Shubham Bagrodia | Senior Software Consultant | Data Engineering
Date: April 22, 2026
Submission Deadline: April 29, 2026 (5 working days)


📋 Assignment Overview
Part
What
Weightage
Part A
5 MCQ Questions
20%
Part B
Coding Activity — Build a Mini Medallion Pipeline
70%
Part C
GitHub Submission Process
10%



Part A: Multiple Choice Questions (5 Questions)
Answer all 5 questions. Each question has exactly one correct answer. Include your answers in the answers.md file as part of your submission.
Question 1
In Medallion Architecture, what is the PRIMARY purpose of the Bronze layer?

A) To aggregate data into business-ready KPIs and dashboards
B) To store raw, unmodified data exactly as received from source systems
C) To deduplicate records and enforce schema constraints
D) To join transactional data with reference/master data
Question 2
A data engineer discovers that the Silver layer has been producing incorrect customer records for the past 2 months due to a bug in the deduplication logic. What is the correct recovery approach in a Medallion Architecture?

A) Re-extract all customer data from the source OLTP database for the past 2 months
B) Manually fix the affected records in the Silver table using UPDATE statements
C) Fix the bug, then reprocess the past 2 months of Bronze data through the corrected Silver transformation logic
D) Delete the Silver table and rebuild Gold directly from Bronze
Question 3
Which of the following is an ANTI-PATTERN in Medallion Architecture?

A) Adding metadata columns (_ingestion_ts, _source_system) to Bronze tables
B) Using MERGE (upsert) operations to incrementally update Silver tables
C) Creating a Gold table that has the exact same granularity and columns as its source Silver table
D) Quarantining records with null primary keys instead of dropping them silently
Question 4
A source system adds a new column discount_percentage to its API response without prior notice. In a well-designed Medallion Architecture, what happens at each layer?

A) Bronze: pipeline crashes. Silver: unaffected. Gold: unaffected.
B) Bronze: captures the new column automatically (schema-on-read). Silver: uses mergeSchema to add the column. Gold: requires a controlled review before adding.
C) Bronze: ignores the new column. Silver: ignores the new column. Gold: ignores the new column.
D) Bronze: captures it. Silver: crashes because schema is enforced. Gold: unaffected.
Question 5
A team needs to build a real-time fraud detection system that must respond within 50 milliseconds. They currently use Medallion Architecture for their analytics platform. What is the BEST approach?

A) Route all fraud detection queries through Gold tables with aggressive caching
B) Replace the entire Medallion Architecture with a Kappa Architecture
C) Keep Medallion for analytics workloads and add a separate real-time Hot Path (e.g., Kafka + Redis) for sub-second fraud detection
D) Use Structured Streaming to reduce Bronze-to-Gold latency to under 50ms


Part B: Coding Activity — Build a Mini Medallion Pipeline
Objective
Build a simplified Medallion Architecture pipeline (Bronze → Silver → Gold) using PySpark that processes sample e-commerce order data. The pipeline should demonstrate deduplication, validation, incremental MERGE, and business aggregation.
Dataset
Use the following sample data (create it as an in-memory DataFrame in your notebook):

# Sample order data with deliberate quality issues

order_data = [

    ("ORD-001", "C10", "Laptop",     "1299.99", "2026-04-10", "North"),

    ("ORD-002", "C11", "Headphones", "199.50",  "2026-04-10", "South"),

    ("ORD-003", "C12", "Keyboard",   "89.99",   "2026-04-11", "East"),

    ("ORD-002", "C11", "Headphones", "199.50",  "2026-04-10", "South"),  # Duplicate

    ("ORD-004", "C13", "Mouse",      "-15.00",  "2026-04-11", "West"),   # Negative amount

    ("ORD-005", None,  "Monitor",    "459.00",  "2026-04-12", "North"),  # Null customer

    ("ORD-006", "C14", "Tablet",     "599.99",  "2026-04-12", "South"),

    ("ORD-007", "C10", "Charger",    "29.99",   "2026-04-13", "North"),

]
Requirements
Your pipeline must implement the following 5 steps:
Step 1: Bronze Layer
Ingest the raw order data into a Bronze Delta table
Add metadata columns: _ingestion_timestamp (current time), _source_system (literal string "ecommerce_api")
Do NOT apply any transformations or filtering — all 8 records including problems must be in Bronze
Step 2: Silver Layer
Read from the Bronze table
Deduplicate: Remove duplicate records based on order_id (ORD-002 appears twice)
Type cast: Convert amount from STRING to DECIMAL(10,2) and order_date from STRING to DATE
Validate: Split records into two outputs:
✅ Valid records (amount >= 0 AND customer_id IS NOT NULL) → Silver table
❌ Invalid records → Quarantine table with a quarantine_reason column explaining why
Step 3: Silver MERGE (Incremental Update)
Simulate a second batch arriving with these 2 new records:

new_orders = [

    ("ORD-001", "C10", "Laptop",  1249.99, "2026-04-10", "North"),  # Price changed!

    ("ORD-008", "C15", "Webcam",  79.99,   "2026-04-14", "East"),   # New order

]

Use DeltaTable.merge() to upsert into the Silver table
ORD-001 should be UPDATED with the new price
ORD-008 should be INSERTED as a new record
Step 4: Gold Layer
Gold Table 1: daily_revenue_by_region — GROUP BY region and order_date, with SUM(amount), COUNT(order_id), AVG(amount)
Gold Table 2: customer_spend — GROUP BY customer_id, with SUM(amount) as total_spend, COUNT(order_id) as total_orders
Step 5: Verification
Print the following counts and verify they match:

Table
Expected Count
Why
Bronze
8
All raw records including problems
Silver (after MERGE)
7
5 original valid + 2 from batch 2 (1 update + 1 insert)
Quarantine
2
1 negative amount + 1 null customer
Gold: daily_revenue
5-6 rows
Depends on region/date combos
Gold: customer_spend
5 rows
5 unique customers after cleanup



Part C: Submission Process
Prerequisites
A GitHub account (create one at github.com if you don't have one)
Git installed on your machine (download from git-scm.com)
A Databricks Community Edition account (free, no credit card) to test your pipeline
Step 1: Fork This Repository
Click the "Fork" button in the top-right corner of this page. This creates a copy of the repo under your own GitHub account.
Step 2: Clone Your Fork Locally
# Replace <your-username> with your GitHub username

git clone https://github.com/<your-username>/medallion-architecture-assignment.git

cd medallion-architecture-assignment
Step 3: Create Your Working Branch
# Create a branch with your name

git checkout -b assignment/<your-name>

# Example:

git checkout -b assignment/rahul-sharma
Step 4: Add Your Files
Create the following file structure inside the repository:

medallion-architecture-assignment/

└── submissions/

    └── <your-name>/

        ├── answers.md              # Your MCQ answers

        ├── medallion_pipeline.py   # Your PySpark pipeline code

        └── README.md              # Brief description of your approach
4a. Create answers.md
Use this format:

# Medallion Architecture - MCQ Answers

**Name:** [Your Full Name]

**Date:** [Submission Date]

## Answers

| Question | Answer | Brief Explanation |

|----------|--------|-------------------|

| Q1       | _      | ... |

| Q2       | _      | ... |

| Q3       | _      | ... |

| Q4       | _      | ... |

| Q5       | _      | ... |

Note: Include a brief 1-line explanation for each answer to demonstrate understanding, not just the letter.
4b. Create medallion_pipeline.py
Your PySpark notebook with all 5 steps from Part B. Make sure it:

Runs end-to-end without errors on Databricks Community Edition
Has clear comments/headers for each step (Bronze, Silver, MERGE, Gold, Verification)
Prints the verification counts at the end
4c. Create README.md
A short README explaining:

Your name and the session date
What the pipeline does (1-2 sentences)
How to run it (e.g., "Paste into a Databricks notebook and run all cells")
Any design decisions or extra features you added
Step 5: Commit and Push
# Stage your files

git add submissions/<your-name>/

# Commit with a descriptive message

git commit -m "Add medallion architecture assignment - <your-name>"

# Push to your fork

git push origin assignment/<your-name>
Step 6: Create a Pull Request
Go to your forked repository on GitHub
You will see a banner: "assignment/<your-name> had recent pushes" with a "Compare & pull request" button — click it
Set the PR title to: Assignment Submission - <Your Full Name>
In the description, briefly list what you implemented and any bonus features
Click "Create pull request"

Important: Your PR is your submission. Do NOT merge it yourself — the session coordinator will review and merge.


⏰ Submission Deadline
Submit your Pull Request within 5 working days of the session — by April 29, 2026.


📊 Evaluation Criteria
Criteria
Weightage
Details
MCQ Answers (with explanations)
20%
Correct answers + reasoning
Bronze Layer Implementation
15%
Raw ingestion + metadata columns
Silver Layer (Cleanse + MERGE)
30%
Dedup, type cast, validation, quarantine, upsert
Gold Layer Aggregations
15%
Correct GROUP BY + business logic
Code Quality & Comments
10%
Clean, readable, well-commented
GitHub Submission Process
10%
Correct branch, PR, file structure



⭐ Bonus (Optional)
For extra credit, implement any of the following:

Time Travel: Use DESCRIBE HISTORY and VERSION AS OF to query a past version of your Silver table
Schema Evolution: Add a discount_pct column to a third batch and use mergeSchema to handle it
Data Quality Report: Print a summary showing how many records passed/failed validation at each layer
Visualization: Create a simple chart of daily revenue by region using matplotlib or Databricks display()


🆘 Need Help?
Resource
Link
Databricks Community Edition
https://community.cloud.databricks.com
Delta Lake Documentation
https://docs.delta.io
Git Basics
https://docs.github.com/en/get-started/quickstart
Questions?
Reach out to Shubh or the Data Engineering competency




Built with ❤️ for NashTech CodeCamp — April 2026

