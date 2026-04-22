from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from datetime import datetime
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
today = datetime.now().strftime('%Y-%m-%d')
today_file = os.path.join(RAW_PATH, f"{today}.jsonl")

if not os.path.exists(today_file):
    print(f"Tidak ada file untuk hari ini: {today_file}")
    spark.stop()
    exit()

print(f"Memproses file: {today_file}")

df = spark.read.schema(schema).json(today_file)
print(f"Total records dibaca: {df.count()}")

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

df_clean.write.jdbc(
    url=POSTGRES_URL,
    table="raw_orders",
    mode="append",
    properties=POSTGRES_PROPS
)

print(f"Data {today} berhasil dimuat ke PostgreSQL!")
spark.stop()