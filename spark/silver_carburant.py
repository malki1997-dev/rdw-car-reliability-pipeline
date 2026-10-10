from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[*]").appName("silver_carburant")
         .config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.parquet("data/bronze/carburant")

# 1 ligne par véhicule : liste triée des carburants + classe hybride
par_vehicule = (
    bronze.groupBy(F.col("kenteken").alias("plate"))
    .agg(
        F.array_sort(F.collect_set("brandstof_omschrijving")).alias("fuels"),
        F.max("klasse_hybride_elektrisch_voertuig").alias("hybrid_class"),
    )
)

has = lambda carburant: F.array_contains("fuels", carburant)

# Règles (l'ordre compte : la première condition vraie gagne)
fuel_type = (
    F.when(has("Waterstof"), "Hydrogen")
     .when(has("Elektriciteit") & (F.size("fuels") > 1), "Hybrid")
     .when(has("Elektriciteit"), "Electric")
     .when(has("LPG"), "LPG")
     .when((F.size("fuels") == 1) & has("Benzine"), "Petrol")
     .when((F.size("fuels") == 1) & has("Diesel"), "Diesel")
     .otherwise("Other")
)

silver = par_vehicule.select(
    "plate",
    fuel_type.alias("fuel_type"),
    F.concat_ws(" + ", "fuels").alias("fuel_list"),
    "hybrid_class",
)

# Écriture en Silver
silver.write.mode("overwrite").parquet("data/silver/carburant")
s = spark.read.parquet("data/silver/carburant")
print(f"[ok] carburant : {s.count():,} lignes")
s.groupBy("fuel_type").count().orderBy(F.desc("count")).show()

spark.stop()
