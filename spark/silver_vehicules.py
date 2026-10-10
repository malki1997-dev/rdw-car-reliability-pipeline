from pyspark.sql import SparkSession, Window, functions as F

spark = (SparkSession.builder.master("local[*]").appName("silver_vehicules")
         .config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def to_date(col):
    return F.to_date(F.try_to_timestamp(F.col(col), F.lit("yyyyMMdd")))


def to_bool(col):
    return F.when(F.col(col) == "Ja", True).when(F.col(col) == "Nee", False)


bronze = spark.read.parquet("data/bronze/vehicules")

# 1. Renommer et typer
df = bronze.select(
    F.col("kenteken").alias("plate"),
    F.col("voertuigsoort").alias("vehicle_type"),
    F.col("merk").alias("brand"),
    F.col("handelsbenaming").alias("model"),
    F.col("inrichting").alias("body_type"),
    to_date("datum_eerste_toelating").alias("first_admission_date"),
    to_date("datum_eerste_tenaamstelling_in_nederland").alias("first_registration_nl_date"),
    to_date("datum_tenaamstelling").alias("last_owner_change_date"),
    F.expr("try_cast(catalogusprijs AS INT)").alias("catalog_price"),
    to_bool("export_indicator").alias("is_exported"),
    to_bool("openstaande_terugroepactie_indicator").alias("has_open_recall"),
    to_bool("taxi_indicator").alias("is_taxi"),
    "_source_file",
    "ingestion_date",
)

# 2. Dédoublonner : garder la version de la page la plus récente
df = df.withColumn("_page", F.regexp_extract("_source_file", r"part-(\d+)", 1).cast("int"))
w = Window.partitionBy("plate").orderBy(F.desc("_page"))
df = (df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_page"))

# 3. Nettoyer le modèle : retirer la marque au début, même répétée (SAAB SAAB 9-3 -> 9-3)
prefixe = F.concat(F.col("brand"), F.lit(" "))
for _ in range(2):
    df = df.withColumn(
        "model",
        F.when(F.col("model").startswith(prefixe),
               F.expr("substring(model, length(brand) + 2)"))
         .otherwise(F.col("model"))
    )

# 4. Écriture en Silver
df.write.mode("overwrite").parquet("data/silver/vehicules")
s = spark.read.parquet("data/silver/vehicules")
print(f"[ok] vehicules : {s.count():,} lignes | plaques uniques : {s.select('plate').distinct().count():,}")

spark.stop()
