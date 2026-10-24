"""Pipeline Silver : Bronze -> Silver (5 tables + table enrichie).

Lancement :
    INGESTION_DATE=2026-10-08 spark-submit --master "local[*]" --driver-memory 4g spark/pipeline_silver.py
"""
import os
import time
from datetime import date

from pyspark.sql import SparkSession, functions as F
from rdw import transforms as t


def main(ingestion_date: str):
    # Pas de master ni de mémoire ici : ils viennent de spark-submit
    spark = SparkSession.builder.appName("pipeline_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    def bronze(name):
        return (spark.read.parquet(f"data/bronze/{name}")
                .filter(F.col("ingestion_date") == ingestion_date))

    def silver(name):
        return spark.read.parquet(f"data/silver/{name}")

    def write(df, name):
        t0 = time.time()
        df.write.mode("overwrite").parquet(f"data/silver/{name}")
        n = silver(name).count()
        print(f"[ok] {name:22} {n:>12,} lignes en {time.time() - t0:.0f} s")

    t_start = time.time()
    write(t.clean_vehicles(bronze("vehicules")), "vehicules")
    write(t.build_fuel_type(bronze("carburant")), "carburant")
    write(t.clean_controles(bronze("controles")), "controles")
    write(t.clean_defauts(bronze("defauts_constates")), "defauts_constates")
    write(t.clean_ref(bronze("ref_defauts")), "ref_defauts")
    write(t.enrich_inspections(silver("controles"), silver("defauts_constates"),
                               silver("vehicules"), silver("carburant")), "inspections_enriched")
    print(f"Pipeline Silver terminé en {time.time() - t_start:.0f} s")
    spark.stop()


if __name__ == "__main__":
    main(os.environ.get("INGESTION_DATE", date.today().isoformat()))
