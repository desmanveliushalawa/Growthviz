from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import os

spark = SparkSession.builder \
    .appName("GlowCart - Process Orders") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_price", LongType(), True),
    StructField("status", StringType(), True),
    StructField("ordered_at", StringType(), True),
])

RAW_PATH = "/opt/airflow/data/raw/orders/"

jsonl_files = [f for f in os.listdir(RAW_PATH) if f.endswith('.jsonl')]

if not jsonl_files:
    print("Tidak ada file .jsonl ditemukan.")
    spark.stop()
    exit()

print(f"Memproses {len(jsonl_files)} file: {jsonl_files}")

df = spark.read.schema(schema).json(RAW_PATH)

df_clean = df \
    .dropDuplicates(["order_id"]) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("total_price") > 0) \
    .withColumn("ordered_at", to_timestamp(col("ordered_at"))) \
    .withColumn("processed_at", current_timestamp())

print(f"Total records bersih: {df_clean.count()}")

POSTGRES_URL = "jdbc:postgresql://postgres:5432/glowcart_db"
POSTGRES_PROPS = {
    "user": "glowcart",
    "password": "glowcart123",
    "driver": "org.postgresql.Driver"
}

df_clean.write \
    .jdbc(
        url=POSTGRES_URL,
        table="raw_orders",
        mode="overwrite",
        properties=POSTGRES_PROPS
    )

print("Data berhasil dimuat ke PostgreSQL!")
spark.stop()