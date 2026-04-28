# MEDALLION ARCHITECTURE PIPELINE

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, DateType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from decimal import Decimal

# STEP 1 — BRONZE LAYER

order_data = [
    ("ORD-001", "C10", "Laptop",     "1299.99", "2026-04-10", "North"),
    ("ORD-002", "C11", "Headphones", "199.50",  "2026-04-10", "South"),
    ("ORD-003", "C12", "Keyboard",   "89.99",   "2026-04-11", "East"),
    ("ORD-002", "C11", "Headphones", "199.50",  "2026-04-10", "South"),  
    ("ORD-004", "C13", "Mouse",      "-15.00",  "2026-04-11", "West"),   
    ("ORD-005", None,  "Monitor",    "459.00",  "2026-04-12", "North"),  
    ("ORD-006", "C14", "Tablet",     "599.99",  "2026-04-12", "South"),
    ("ORD-007", "C10", "Charger",    "29.99",   "2026-04-13", "North"),
]

bronze_schema = StructType([
    StructField("order_id",    StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product",     StringType(), True),
    StructField("amount",      StringType(), True), 
    StructField("order_date",  StringType(), True),  
    StructField("region",      StringType(), True),
])

raw_df = spark.createDataFrame(order_data, schema=bronze_schema)

bronze_df = raw_df \
    .withColumn("_ingestion_timestamp", F.current_timestamp()) \
    .withColumn("_source_system", F.lit("ecommerce_api"))

# overwrite replaces table on re-runs
bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_orders")

print(f" Bronze table.")
spark.table("bronze_orders").show(truncate=False)

# STEP 2 — SILVER LAYER

bronze_read = spark.table("bronze_orders")

# Deduplicate on order_id
dedup_window = Window.partitionBy("order_id").orderBy("_ingestion_timestamp")

deduped_df = bronze_read \
    .withColumn("_row_num", F.row_number().over(dedup_window)) \
    .filter(F.col("_row_num") == 1) \
    .drop("_row_num")

# Type cast
typed_df = deduped_df \
    .withColumn("amount",     F.col("amount").cast(DecimalType(10, 2))) \
    .withColumn("order_date", F.col("order_date").cast(DateType()))

# Tag each record with quarantine reason
validated_df = typed_df.withColumn(
    "quarantine_reason",
    F.when(F.col("customer_id").isNull() & (F.col("amount") < 0),
           F.lit("null customer_id AND negative amount"))
     .when(F.col("customer_id").isNull(),
           F.lit("null customer_id"))
     .when(F.col("amount") < 0,
           F.lit("negative amount"))
     .otherwise(F.lit(None).cast(StringType()))
)

#Split valid and quarantine 
valid_df = validated_df \
    .filter(F.col("quarantine_reason").isNull()) \
    .drop("quarantine_reason", "_ingestion_timestamp", "_source_system")

quarantine_df = validated_df \
    .filter(F.col("quarantine_reason").isNotNull())

valid_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_orders")

quarantine_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_orders_quarantine")

print("\n── Silver valid records ──")
spark.table("silver_orders").show(truncate=False)

print("── Quarantine records ──")
spark.table("silver_orders_quarantine") \
    .select("order_id", "customer_id", "amount", "quarantine_reason") \
    .show(truncate=False)



# STEP 3  SILVER MERGE

new_orders = [
    ("ORD-001", "C10", "Laptop", Decimal("1249.99"), "2026-04-10", "North"),  # price update
    ("ORD-008", "C15", "Webcam", Decimal("79.99"),   "2026-04-14", "East"),   # new record
]

new_schema = StructType([
    StructField("order_id",    StringType(),      True),
    StructField("customer_id", StringType(),      True),
    StructField("product",     StringType(),      True),
    StructField("amount",      DecimalType(10,2), True),
    StructField("order_date",  StringType(),      True),
    StructField("region",      StringType(),      True),
])

new_batch_df = spark.createDataFrame(new_orders, schema=new_schema) \
    .withColumn("order_date", F.col("order_date").cast(DateType()))

silver_delta = DeltaTable.forName(spark, "silver_orders")

silver_delta.alias("silver") \
    .merge(
        new_batch_df.alias("batch"),
        "silver.order_id = batch.order_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

silver_after = spark.table("silver_orders")

print("\n── Silver after MERGE ──\n new record ORD-008 added and ORD-001 price updated")
silver_after.orderBy("order_id").show(truncate=False)

# STEP 4  GOLD LAYER

silver_df = spark.table("silver_orders")

# Gold Table: daily_revenue
daily_revenue_df = silver_df \
    .groupBy("region", "order_date") \
    .agg(
        F.sum("amount").cast(DecimalType(12, 2)).alias("total_revenue"),
        F.count("order_id").alias("order_count"),
        F.avg("amount").cast(DecimalType(10, 2)).alias("avg_order_value")
    ) \
    .orderBy("order_date", "region")

daily_revenue_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_daily_revenue_by_region")
print(f"Gold: daily_revenue")
daily_revenue_df.show(truncate=False)

# Gold Table: customer_spend
customer_spend_df = silver_df \
    .groupBy("customer_id") \
    .agg(
        F.sum("amount").cast(DecimalType(12, 2)).alias("total_spend"),
        F.count("order_id").alias("total_orders")
    ) \
    .orderBy("customer_id")

customer_spend_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_customer_spend")
print(f"Gold: customer_spend.")
customer_spend_df.show(truncate=False)

# Verification
print("\n=== FINAL VERIFICATION ===")

print("Bronze count:", spark.table("bronze_orders").count())
print("Silver count:", spark.table("silver_orders").count())  
print("Quarantine count:", spark.table("silver_orders_quarantine").count())  
print("Gold daily revenue count:", spark.table("gold_daily_revenue_by_region").count())
print("Gold customer spend count:", spark.table("gold_customer_spend").count()) 
