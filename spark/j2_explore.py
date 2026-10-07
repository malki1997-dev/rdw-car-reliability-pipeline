from pyspark.sql import SparkSession

# 1. Créer la SparkSession (point d'entrée)
spark = (
    SparkSession.builder
    .master("local[*]")          # Spark tourne sur ton PC, avec tous les cœurs
    .appName("j2_explore")       # nom visible dans le Spark UI
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")   # moins de messages dans le terminal

# 2. Lire le CSV des véhicules
df = (
    spark.read
    .option("header", True)      # la 1re ligne contient les noms de colonnes
    .csv("data/landing/sample_vehicules.csv")
)

# 3. Afficher 5 lignes et 4 colonnes
df.select("kenteken", "merk", "handelsbenaming", "voertuigsoort").show(5)

# 4. Compter les lignes
print("Nombre de lignes :", df.count())

print("=== SANS inferSchema ===")
df.select("kenteken", "catalogusprijs", "datum_eerste_toelating").printSchema()

df_infer = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/landing/sample_vehicules.csv")
)
print("=== AVEC inferSchema ===")
df_infer.select("kenteken", "catalogusprijs", "datum_eerste_toelating").printSchema()

from pyspark.sql import functions as F

print("=== Voitures particulières uniquement ===")
voitures = (
    df.select("kenteken", "merk", "handelsbenaming", "voertuigsoort", "datum_eerste_toelating")
      .filter(F.col("voertuigsoort") == "Personenauto")
      .withColumn("annee", F.substring("datum_eerste_toelating", 1, 4))
)
voitures.show(5)
print("Nombre de voitures :", voitures.count())

import time

print("=== Lazy evaluation ===")
t0 = time.time()
toyota = df.filter(F.col("merk") == "TOYOTA").select("kenteken", "handelsbenaming")
print("Temps des transformations :", round(time.time() - t0, 3), "s")

t0 = time.time()
print("Nombre de Toyota :", toyota.count())
print("Temps de l'action :", round(time.time() - t0, 3), "s")

toyota.explain()

print("=== Top 10 marques (voitures) ===")
top_marques = (
    voitures.groupBy("merk")
            .agg(F.count("*").alias("nb_voitures"))
            .orderBy(F.desc("nb_voitures"))
)
top_marques.show(10)

input("Spark UI : http://localhost:4040  —  appuie sur Entrée pour terminer...")

spark.stop()
