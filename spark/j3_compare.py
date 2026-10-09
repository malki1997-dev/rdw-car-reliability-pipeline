import time
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[*]").appName("j3_compare")
         .config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

def top_marques(df, label):
    t0 = time.time()
    df.groupBy("merk").count().orderBy(F.desc("count")).limit(5).collect()
    print(f"{label:8} : {time.time() - t0:.1f} s")

csv = spark.read.option("header", True).csv("data/landing/vehicules/ingestion_date=2026-10-08/")
parquet = spark.read.parquet("data/bronze/vehicules")

top_marques(csv, "CSV")
top_marques(parquet, "Parquet")
spark.stop()
