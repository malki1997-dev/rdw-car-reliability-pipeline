from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[*]").appName("j3_doublons")
         .config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("data/bronze/vehicules")
colonnes_metier = [c for c in df.columns if c not in ("_source_file", "ingestion_date")]

# Plaques présentes plusieurs fois
dups = df.groupBy("kenteken").count().filter("count > 1").select("kenteken")
lignes = df.join(dups, "kenteken")

# Les doublons sont-ils identiques (mêmes valeurs métier) ?
nb_versions = lignes.select(*colonnes_metier).distinct().groupBy("kenteken").count()
print("Plaques en double                  :", f"{dups.count():,}")
print("dont lignes 100% identiques        :", f"{nb_versions.filter('count = 1').count():,}")
print("dont valeurs différentes           :", f"{nb_versions.filter('count > 1').count():,}")

# Exemple : de quelles pages viennent-ils ?
exemple = dups.limit(3)
(lignes.join(exemple, "kenteken")
       .select("kenteken", "merk", "handelsbenaming",
               F.regexp_extract("_source_file", r"part-(\d+)", 1).alias("page"))
       .orderBy("kenteken").show(truncate=False))
spark.stop()
