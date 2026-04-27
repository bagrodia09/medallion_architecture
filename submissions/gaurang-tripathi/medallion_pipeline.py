# Databricks notebook source
print(spark)
from delta.tables import DeltaTable
print("Delta working")

# COMMAND ----------

raw_orders = [
    ("ORD-001", "C10", "Laptop", "1299.99", "2026-04-10", "North"),
    ("ORD-002", "C11", "Headphones", "199.50", "2026-04-10", "South"),
    ("ORD-003", "C12", "Keyboard", "89.99", "2026-04-11", "East"),
    ("ORD-002", "C11", "Headphones", "199.50", "2026-04-10", "South"),  # duplicate
    ("ORD-004", "C13", "Mouse", "-15.00", "2026-04-11", "West"),         # negative
    ("ORD-005", None, "Monitor", "459.00", "2026-04-12", "North"),       # null customer
    ("ORD-006", "C14", "Tablet", "599.99", "2026-04-12", "South"),
    ("ORD-007", "C10", "Charger", "29.99", "2026-04-13", "North")
]

# COMMAND ----------

from pyspark.sql.functions import *

schema = ["order_id","customer_id","product","amount","order_date","region"]

bronze_df = spark.createDataFrame(raw_orders, schema)

# COMMAND ----------

bronze_df = bronze_df.withColumn(
    "_ingestion_timestamp",
    current_timestamp()
).withColumn(
    "_source_system",
    lit("ecommerce_api")
)

# COMMAND ----------

bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze_orders")

# COMMAND ----------

spark.table("bronze_orders").show()

# COMMAND ----------

bronze_data = spark.table("bronze_orders")
bronze_data.show()

# COMMAND ----------

dedup_df = bronze_data.dropDuplicates(["order_id"])
dedup_df.count()


# COMMAND ----------

from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col,to_date

silver_cast_df = dedup_df.withColumn(
    "amount",
    col("amount").cast(DecimalType(10,2))
).withColumn(
    "order_date",
    to_date("order_date")
)

silver_cast_df.show()

# COMMAND ----------

valid_df = silver_cast_df.filter(
    (col("amount") >= 0) &
    (col("customer_id").isNotNull())
)

valid_df.show()
valid_df.count()

# COMMAND ----------

from pyspark.sql.functions import when

quarantine_df = silver_cast_df.filter(
    (col("amount") < 0) |
    (col("customer_id").isNull())
).withColumn(
    "quarantine_reason",
    when(col("amount") < 0, "Negative amount")
    .when(col("customer_id").isNull(), "Null customer")
)

quarantine_df.show()
quarantine_df.count()

# COMMAND ----------

valid_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_orders")

# COMMAND ----------

quarantine_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("quarantine_orders")

# COMMAND ----------


spark.table("quarantine_orders").count()

# COMMAND ----------

spark.table("silver_orders").count()

# COMMAND ----------

new_orders = [
    ("ORD-001", "C10", "Laptop", 1249.99, "2026-04-10", "North"),
    ("ORD-008", "C15", "Webcam", 79.99, "2026-04-14", "East")
]

# COMMAND ----------

new_schema = [
    "order_id",
    "customer_id",
    "product",
    "amount",
    "order_date",
    "region"
]

new_df = spark.createDataFrame(new_orders, new_schema)
new_df.show()

# COMMAND ----------

from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col,to_date

new_df = new_df.withColumn(
    "amount",
    col("amount").cast(DecimalType(10,2))
).withColumn(
    "order_date",
    to_date("order_date")
)

new_df.show()

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "silver_orders")

delta_table.alias("target").merge(
    new_df.alias("source"),
    "target.order_id = source.order_id"
)\
.whenMatchedUpdate(
    set={
        "customer_id": "source.customer_id",
        "product": "source.product",
        "amount": "source.amount",
        "order_date": "source.order_date",
        "region": "source.region"
    }
)\
.whenNotMatchedInsert(
    values={
        "order_id": "source.order_id",
        "customer_id": "source.customer_id",
        "product": "source.product",
        "amount": "source.amount",
        "order_date": "source.order_date",
        "region": "source.region",
        "_ingestion_timestamp": "current_timestamp()",
        "_source_system": "'ecommerce_api'"
    }
)\
.execute()

# COMMAND ----------

spark.table("silver_orders").show()

# COMMAND ----------

spark.table("silver_orders").count()

# COMMAND ----------

spark.table("silver_orders").filter(
    col("order_id") == "ORD-001"
).show()

# COMMAND ----------

silver_final = spark.table("silver_orders")
silver_final.show()

# COMMAND ----------

from pyspark.sql.functions import sum, avg, count

daily_revenue_df = silver_final.groupBy(
    "region",
    "order_date"
).agg(
    sum("amount").alias("total_revenue"),
    count("order_id").alias("total_orders"),
    avg("amount").alias("avg_amount")
)

# COMMAND ----------

daily_revenue_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_daily_revenue")

# COMMAND ----------

spark.table("gold_daily_revenue").show()

# COMMAND ----------

customer_spend_df = silver_final.groupBy(
    "customer_id"
).agg(
    sum("amount").alias("total_spend"),
    count("order_id").alias("total_orders")
)

# COMMAND ----------

customer_spend_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_customer_spend")

# COMMAND ----------

spark.table("gold_customer_spend").show()

# COMMAND ----------

print("Bronze Count:", spark.table("bronze_orders").count())
print("Silver Count:", spark.table("silver_orders").count())
print("Quarantine Count:", spark.table("quarantine_orders").count())
print("Gold Revenue Count:", spark.table("gold_daily_revenue").count())
print("Gold Customer Count:", spark.table("gold_customer_spend").count())