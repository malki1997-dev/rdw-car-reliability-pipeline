"""Charge les tables Silver dans PostgreSQL (schéma raw).

Lancement (toutes les tables, ou seulement celles passées en argument) :
    set -a; source .env; set +a
    spark-submit --master "local[4]" --driver-memory 3g \
        --packages org.postgresql:postgresql:42.7.4 spark/load_postgres.py [table ...]
"""
import os
import sys
import time

from pyspark.sql import SparkSession

# table Silver -> colonnes techniques à ne pas charger
TABLES = {
    "ref_defauts": [],
    "carburant": [],
    "vehicules": ["_source_file"],
    "defauts_constates": [],
    "inspections_enriched": [],
}


def main(noms):
    spark = SparkSession.builder.appName("load_postgres").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Secrets lus depuis l'environnement (fichier .env), jamais écrits dans le code
    url = os.environ["PG_URL"] + "?reWriteBatchedInserts=true"
    props = {
        "user": os.environ["PG_USER"],
        "password": os.environ["PG_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }

    for name in noms:
        t0 = time.time()
        df = spark.read.parquet(f"data/silver/{name}").drop(*TABLES[name])
        (df.coalesce(4)                          # 4 connexions, SANS shuffle
           .write.mode("overwrite")              # idempotent : la table est recréée
           .option("batchsize", 10000)
           .jdbc(url, f"raw.{name}", properties=props))
        print(f"[ok] raw.{name:22} {df.count():>12,} lignes en {time.time() - t0:.0f} s")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:] or list(TABLES))
