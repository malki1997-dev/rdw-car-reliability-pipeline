import os
import sys
import time
from datetime import date
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

COLONNES = {
    "vehicules": ["kenteken", "voertuigsoort", "merk", "handelsbenaming", "inrichting",
                  "datum_eerste_toelating", "datum_eerste_tenaamstelling_in_nederland",
                  "datum_tenaamstelling", "catalogusprijs", "export_indicator",
                  "openstaande_terugroepactie_indicator", "taxi_indicator"],
    "carburant": ["kenteken", "brandstof_volgnummer", "brandstof_omschrijving",
                  "klasse_hybride_elektrisch_voertuig"],
    "controles": ["kenteken", "soort_erkenning_keuringsinstantie",
                  "meld_datum_door_keuringsinstantie", "meld_tijd_door_keuringsinstantie",
                  "soort_erkenning_omschrijving", "soort_melding_ki_omschrijving",
                  "vervaldatum_keuring"],
    "defauts_constates": ["kenteken", "soort_erkenning_keuringsinstantie",
                          "meld_datum_door_keuringsinstantie", "meld_tijd_door_keuringsinstantie",
                          "gebrek_identificatie", "soort_erkenning_omschrijving",
                          "aantal_gebreken_geconstateerd"],
    "ref_defauts": ["gebrek_identificatie", "ingangsdatum_gebrek", "einddatum_gebrek",
                    "gebrek_paragraaf_nummer", "gebrek_artikel_nummer", "gebrek_omschrijving",
                    "ingangsdatum_gebrek_dt", "einddatum_gebrek_dt"],
}

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("bronze_ingest")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # n'écrase que la date traitée
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def ingest(name, ingestion_date):
    t0 = time.time()
    schema = StructType([StructField(c, StringType(), True) for c in COLONNES[name]])
    source = f"data/landing/{name}/ingestion_date={ingestion_date}/"

    df = (
        spark.read
        .option("header", True)
        .option("enforceSchema", False)            # vérifie le header contre le schéma
        .schema(schema)
        .csv(source)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_date", F.lit(ingestion_date))
    )

    (
        df.write
        .mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(f"data/bronze/{name}")
    )
    n = spark.read.parquet(f"data/bronze/{name}").filter(F.col("ingestion_date") == ingestion_date).count()
    print(f"[ok] {name:18} : {n:,} lignes en {time.time() - t0:.0f} s")


if __name__ == "__main__":
    names = sys.argv[1:] or list(COLONNES)
    ingestion_date = os.environ.get("INGESTION_DATE", date.today().isoformat())
    for name in names:
        ingest(name, ingestion_date)
    spark.stop()
