from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[*]").appName("j5_enrichi")
         .config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

CLE = ["plate", "approval_type", "inspection_date", "inspection_time"]

controles = spark.read.parquet("data/silver/controles")
defauts = spark.read.parquet("data/silver/defauts_constates")
vehicules = spark.read.parquet("data/silver/vehicules").select(
    "plate", "vehicle_type", "brand", "model", "first_admission_date")
carburant = spark.read.parquet("data/silver/carburant").select("plate", "fuel_type")

# 1. Défauts agrégés : 1 ligne par contrôle
defauts_par_controle = defauts.groupBy(*CLE).agg(
    F.count("*").alias("nb_defect_types"),
    F.sum("defect_count").alias("nb_defects"))

res = (controles
       .join(defauts_par_controle, CLE, "left")
       .fillna({"nb_defect_types": 0, "nb_defects": 0})
       # 2. + véhicule
       .join(vehicules, "plate", "left")
       # 3. + carburant
       .join(carburant, "plate", "left")
       .fillna({"fuel_type": "None"})
       # 4. Colonnes calculées
       .withColumn("vehicle_age_years",
                   F.round(F.months_between("inspection_date", "first_admission_date") / 12, 1))
       .withColumn("is_apk", F.col("approval_description").startswith("APK"))
       .withColumn("has_defect", F.col("nb_defects") > 0))

# 5. Écriture
res.write.mode("overwrite").parquet("data/silver/inspections_enriched")

# Contrôles
s = spark.read.parquet("data/silver/inspections_enriched")
print(f"[ok] inspections_enriched : {s.count():,} lignes")
s.groupBy("is_apk").count().show()
s.groupBy("fuel_type").count().orderBy(F.desc("count")).show()

spark.stop()
