from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[*]").appName("j4_profile")
         .config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("data/bronze/vehicules")
total = df.count()
print(f"Total : {total:,} lignes\n")

# 1. Pourcentage de valeurs nulles par colonne
print("=== % de nulls ===")
for c in df.columns:
    n = df.filter(F.col(c).isNull() | (F.trim(F.col(c)) == "")).count()
    print(f"{c:45} {100 * n / total:6.2f} %")

# 2. Types de véhicules
print("\n=== voertuigsoort ===")
df.groupBy("voertuigsoort").count().orderBy(F.desc("count")).show(truncate=False)

# 3. Dates au mauvais format (doivent être 8 chiffres : yyyyMMdd)
mauvais = df.filter(~F.col("datum_eerste_toelating").rlike(r"^\d{8}$")).count()
print(f"Dates de 1re mise en circulation invalides : {mauvais:,}")

# 4. Modèles qui commencent par la marque (ex : NISSAN QASHQAI)
avec_marque = df.filter(F.col("handelsbenaming").startswith(F.col("merk"))).count()
print(f"Modèles qui commencent par la marque       : {avec_marque:,}")

# 5. Valeurs des indicateurs
print("\n=== Indicateurs ===")
for c in ["export_indicator", "openstaande_terugroepactie_indicator", "taxi_indicator"]:
    df.groupBy(c).count().show()

spark.stop()
