# Databricks notebook source
# Databricks notebook source

# =====================================================
# Medallion Architecture Assignment
# Name: sabia parveen
# Session Date: 27-Apr-2026
# Description:
# Bronze → Silver → Gold Pipeline using PySpark + Delta Lake
# =====================================================
# STEP 0: IMPORT LIBRARIES
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable




# STEP 1: CREATE SAMPLE SOURCE DATA
order_data = [
("ORD-001", "C10", "Laptop", "1299.99", "2026-04-10", "North"),
("ORD-002", "C11", "Headphones", "199.50", "2026-04-10", "South"),
("ORD-003", "C12", "Keyboard", "89.99", "2026-04-11", "East"),
("ORD-002", "C11", "Headphones", "199.50", "2026-04-10", "South"),  # Duplicate
("ORD-004", "C13", "Mouse", "-15.00", "2026-04-11", "West"),   # Negative amount
("ORD-005", None, "Monitor", "459.00", "2026-04-12", "North"), # Null customer
("ORD-006", "C14", "Tablet", "599.99", "2026-04-12", "South"),
("ORD-007", "C10", "Charger", "29.99", "2026-04-13", "North")
]

cols = ["order_id","customer_id","product","amount","order_date","region"]

df = spark.createDataFrame(order_data, cols)
print("Raw Source Data")
df.show()



# STEP 2: BRONZE LAYER
# Store raw data exactly as received + metadata columns
bronze_df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
              .withColumn("_source_system", lit("ecommerce_api"))

bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze_orders")



print("Bronze Table Created")
spark.table("bronze_orders").show()



# STEP 3: SILVER LAYER
# Read Bronze Data
bronze = spark.table("bronze_orders")



# STEP 3A: REMOVE DUPLICATES
dedup = bronze.dropDuplicates(["order_id"])



# STEP 3B: TYPE CASTING
cleaned = dedup.withColumn("amount", col("amount").cast("decimal(10,2)")) \
               .withColumn("order_date", to_date("order_date"))



# STEP 3C: VALIDATION + QUARANTINE
validated = cleaned.withColumn(
    "quarantine_reason",
    when(col("customer_id").isNull(), "customer_id is null")
    .when(col("amount") < 0, "negative amount")
)
# Valid records → Silver
silver_df = validated.filter(col("quarantine_reason").isNull()) \
                     .drop("quarantine_reason")
# Invalid records → Quarantine
quarantine_df = validated.filter(col("quarantine_reason").isNotNull())



# STEP 3D: SAVE SILVER + QUARANTINE TABLES
silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_orders")

quarantine_df.write.format("delta").mode("overwrite").saveAsTable("quarantine_orders")



print("Silver Count:", spark.table("silver_orders").count())
print("Quarantine Count:", spark.table("quarantine_orders").count())



# STEP 4: SILVER MERGE (INCREMENTAL UPDATE)
# Simulate second batch data
new_orders = [
("ORD-001", "C10", "Laptop", 1249.99, "2026-04-10", "North"),
("ORD-008", "C15", "Webcam", 79.99, "2026-04-14", "East")
]

new_df = spark.createDataFrame(new_orders,
["order_id","customer_id","product","amount","order_date","region"])

new_df = new_df.withColumn("amount", col("amount").cast("decimal(10,2)")) \
               .withColumn("order_date", to_date("order_date"))



from pyspark.sql.functions import current_timestamp, lit

new_df = new_df.withColumn("_ingestion_timestamp", current_timestamp()) \
               .withColumn("_source_system", lit("ecommerce_api"))
# STEP 4A: MERGE INTO SILVER TABLE
deltaTable = DeltaTable.forName(spark, "silver_orders")

deltaTable.alias("target").merge(
    new_df.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdate(set={
    "customer_id": "source.customer_id",
    "product": "source.product",
    "amount": "source.amount",
    "order_date": "source.order_date",
    "region": "source.region",
    "_ingestion_timestamp": "source._ingestion_timestamp",
    "_source_system": "source._source_system"
}).whenNotMatchedInsertAll() \
.execute()



print("MERGE Completed")
spark.table("silver_orders").show()



# STEP 5: GOLD LAYER
# Gold Table 1 = Daily Revenue by Region
daily = spark.table("silver_orders").groupBy("region","order_date").agg(
sum("amount").alias("total_revenue"),
count("order_id").alias("total_orders"),
avg("amount").alias("avg_amount")
)

daily.write.format("delta").mode("overwrite").saveAsTable("gold_daily_revenue")



# STEP 5A: Gold Table 2 = Customer Spend
customer = spark.table("silver_orders").groupBy("customer_id").agg(
sum("amount").alias("total_spend"),
count("order_id").alias("total_orders")
)

customer.write.format("delta").mode("overwrite").saveAsTable("gold_customer_spend")



# STEP 6: VERIFICATION COUNTS
print("Bronze count:", spark.table("bronze_orders").count())
print("Silver count:", spark.table("silver_orders").count())
print("Quarantine count:", spark.table("quarantine_orders").count())
print("Gold Daily count:", spark.table("gold_daily_revenue").count())
print("Gold Customer count:", spark.table("gold_customer_spend").count())

# STEP 7: DISPLAY FINAL TABLES

print("Silver Orders")
spark.table("silver_orders").show()

print("Gold Daily Revenue")
spark.table("gold_daily_revenue").show()

print("Gold Customer Spend")
spark.table("gold_customer_spend").show()