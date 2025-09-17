import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as _sum, count as _count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

PARQUET_OUT = "output/totals_parquet"
CSV_OUT      = "output/totals_csv"
CHECKPOINT   = "output/checkpoint"

schema = StructType([
    StructField("order_id",   StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price",      DoubleType(), True),
    StructField("timestamp",  StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("online-store-totals")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "org.apache.hadoop:hadoop-client-api:3.3.5,"
        "org.apache.hadoop:hadoop-client-runtime:3.3.5"
    )
    .config("spark.driver.extraJavaOptions",  "-Dhadoop.home.dir=C:/hadoop")
    .config("spark.executor.extraJavaOptions","-Dhadoop.home.dir=C:/hadoop")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1) Kafka -> raw
raw = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "orders")
         .option("startingOffsets", "latest")
         .load()
)

parsed = (
    raw.select(from_json(col("value").cast("string"), schema).alias("j"))
       .select("j.*")
       .withColumn("product_id", col("product_id").cast("string"))
       .withColumn("price", col("price").cast("double"))
)

# 2) Agregacja
totals = (
    parsed.groupBy("product_id")
          .agg(
              _count("*").alias("order_count"),
              _sum("price").alias("total_revenue")
          )
)

# 3) Podgląd w konsoli

console_q = (
    totals.orderBy(col("total_revenue").desc())
          .writeStream
          .outputMode("complete")
          .format("console")
          .option("truncate", "false")
          .start()
)

# 4A) PARQUET (foreachBatch)
def save_parquet(df, batch_id):
    (df.orderBy(col("total_revenue").desc())
       .coalesce(1)
       .write.mode("overwrite")
       .parquet(PARQUET_OUT))

parquet_q = (
    totals.writeStream
          .outputMode("complete")
          .foreachBatch(save_parquet)
          .option("checkpointLocation", CHECKPOINT + "_parquet")
          .start()
)

# 4B) CSV (foreachBatch) — każdy batch do osobnego katalogu
def save_csv(df, batch_id):
    batch_dir = os.path.join(CSV_OUT, f"batch_{batch_id}")
    (df.orderBy(col("total_revenue").desc())
       .coalesce(1)
       .write.mode("overwrite")
       .option("header", True)
       .csv(batch_dir))

csv_q = (
    totals.writeStream
          .outputMode("complete")
          .foreachBatch(save_csv)
          .option("checkpointLocation", CHECKPOINT + "_csv")
          .start()
)

console_q.awaitTermination()
parquet_q.awaitTermination()
csv_q.awaitTermination()



