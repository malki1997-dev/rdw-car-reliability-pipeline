import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("j3_read_landing")
    .config("spark.driver.memory", "4g")   # 4 Go de RAM pour Spark
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Schéma explicite : toutes les colonnes en texte (Bronze = données brutes)
colonnes = [
    "kenteken", "soort_erkenning_keuringsinstantie",
    "meld_datum_door_keuringsinstantie", "meld_tijd_door_keuringsinstantie",
    "soort_erkenning_omschrijving", "soort_melding_ki_omschrijving",
    "vervaldatum_keuring",
]
schema = StructType([StructField(c, StringType(), True) for c in colonnes])

t0 = time.time()
df = (
    spark.read
    .option("header", True)
    .schema(schema)                         # pas d'inferSchema
    .csv("data/landing/controles/ingestion_date=2026-10-08/")
)

df.show(5)
print("Nombre de lignes     :", f"{df.count():,}")
print("Nombre de partitions :", df.rdd.getNumPartitions())
print("Temps                :", round(time.time() - t0, 1), "s")

spark.stop()
