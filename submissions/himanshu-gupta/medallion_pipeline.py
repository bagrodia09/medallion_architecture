from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.functions import to_date
from delta.tables import DeltaTable
from pyspark.sql.functions import sum, count, avg

spark = SparkSession.builder.getOrCreate()

# Source data (as provided)
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

columns = [
    "order_id",
    "customer_id",
    "product",
    "order_amount",
    "order_date",
    "region"
]

# Create DataFrame
df = spark.createDataFrame(order_data, columns)

# Add Bronze metadata columns
bronze_df = (
    df
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_system", lit("ecommerce_api"))
)

# Write to Bronze Delta table
bronze_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("bronze.order_events")

# Read from Bronze table
bronze_df = spark.table("bronze.order_events")

# Deduplicate on order_id (keep first occurrence)
dedup_df = bronze_df.dropDuplicates(["order_id"])

# Type casting
typed_df = (
    dedup_df
    .withColumn("order_amount", col("order_amount").cast(DecimalType(10, 2)))
    .withColumn("order_date", to_date(col("order_date")))
)

# Validation conditions
valid_condition = (
    (col("order_amount") >= 0) &
    (col("customer_id").isNotNull())
)

# Split into Valid (Silver) and Invalid (Quarantine)
silver_df = typed_df.filter(valid_condition)

quarantine_df = (
    typed_df
    .filter(~valid_condition)
    .withColumn(
        "quarantine_reason",
        when(col("order_amount") < 0, "NEGATIVE_ORDER_AMOUNT")
        .when(col("customer_id").isNull(), "NULL_CUSTOMER_ID")
        .otherwise("UNKNOWN_REASON")
    )
)

# Write Silver table
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.orders")

# Write Quarantine table
quarantine_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.orders_quarantine")

new_orders = [
    ("ORD-001", "C10", "Laptop",  1249.99, "2026-04-10", "North"),  # Updated price
    ("ORD-008", "C15", "Webcam",   79.99,  "2026-04-14", "East"),  # New order
]

incremental_df = (
    spark.createDataFrame(new_orders, columns)
         .withColumn("order_date", to_date("order_date"))
)

silver_table = DeltaTable.forName(spark, "silver.orders")

(
    silver_table.alias("target")
    .merge(
        incremental_df.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdate(set={
        "customer_id": "source.customer_id",
        "product": "source.product",
        "order_amount": "source.order_amount",
        "order_date": "source.order_date",
        "region": "source.region"
    })
    .whenNotMatchedInsert(values={
        "order_id": "source.order_id",
        "customer_id": "source.customer_id",
        "product": "source.product",
        "order_amount": "source.order_amount",
        "order_date": "source.order_date",
        "region": "source.region"
    })
    .execute()
)

silver_df = spark.table("silver.orders")

daily_revenue_by_region_df = (
    silver_df
    .groupBy("region", "order_date")
    .agg(
        sum("order_amount").alias("total_revenue"),
        count("order_id").alias("total_orders"),
        avg("order_amount").alias("avg_order_value")
    )
)

daily_revenue_by_region_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.daily_revenue_by_region")

customer_spend_df = (
    silver_df
    .groupBy("customer_id")
    .agg(
        sum("order_amount").alias("total_spend"),
        count("order_id").alias("total_orders")
    )
)

customer_spend_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.customer_spend")

# Verification
bronze_df = spark.table("bronze.order_events")
assert bronze_df.count() == 8

silver_df = spark.table("silver.orders")
assert silver_df.count() == 6

silver_df_quarantined = spark.table("silver.orders_quarantine")
assert silver_df_quarantined.count() == 2

gold_daily_revenue_by_region_df = spark.table("gold.daily_revenue_by_region")
assert gold_daily_revenue_by_region_df.count() == 6

gold_customer_spend_df = spark.table("gold.customer_spend")
assert gold_customer_spend_df.count() == 5