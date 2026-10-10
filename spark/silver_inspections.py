from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[*]").appName("silver_inspections")
         .config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def to_date(col):
    return F.to_date(F.try_to_timestamp(F.col(col), F.lit("yyyyMMdd")))


# --- Contrôles ---
b = spark.read.parquet("data/bronze/controles")
controles = b.select(
    F.col("kenteken").alias("plate"),
    F.col("soort_erkenning_keuringsinstantie").alias("approval_type"),
    to_date("meld_datum_door_keuringsinstantie").alias("inspection_date"),
    F.col("meld_tijd_door_keuringsinstantie").alias("inspection_time"),
    F.col("soort_erkenning_omschrijving").alias("approval_description"),
    F.col("soort_melding_ki_omschrijving").alias("report_type"),
    to_date("vervaldatum_keuring").alias("expiry_date"),
)

# --- Défauts constatés ---
b = spark.read.parquet("data/bronze/defauts_constates")
defauts = b.select(
    F.col("kenteken").alias("plate"),
    F.col("soort_erkenning_keuringsinstantie").alias("approval_type"),
    to_date("meld_datum_door_keuringsinstantie").alias("inspection_date"),
    F.col("meld_tijd_door_keuringsinstantie").alias("inspection_time"),
    F.col("gebrek_identificatie").alias("defect_code"),
    F.expr("try_cast(aantal_gebreken_geconstateerd AS INT)").alias("defect_count"),
)

# --- Référentiel des défauts ---
b = spark.read.parquet("data/bronze/ref_defauts")
ref = b.select(
    F.col("gebrek_identificatie").alias("defect_code"),
    to_date("ingangsdatum_gebrek").alias("valid_from"),
    to_date("einddatum_gebrek").alias("valid_to"),
    F.col("gebrek_paragraaf_nummer").alias("paragraph_number"),
    F.col("gebrek_artikel_nummer").alias("article_number"),
    F.col("gebrek_omschrijving").alias("defect_description"),
)

# Écriture + contrôles
for nom, df, cols_a_verifier in [
    ("controles", controles, ["inspection_date", "expiry_date"]),
    ("defauts_constates", defauts, ["inspection_date", "defect_count"]),
    ("ref_defauts", ref, ["valid_from", "defect_description"]),
]:
    df.write.mode("overwrite").parquet(f"data/silver/{nom}")
    s = spark.read.parquet(f"data/silver/{nom}")
    nulls = " | ".join(f"{c} NULL={s.filter(F.col(c).isNull()).count():,}" for c in cols_a_verifier)
    print(f"[ok] {nom:18} {s.count():>12,} lignes | {nulls}")

spark.stop()
