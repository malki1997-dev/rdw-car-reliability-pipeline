from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local[*]").appName("j3_check_keys")
         .config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

CLE_CONTROLE = ["kenteken", "soort_erkenning_keuringsinstantie",
                "meld_datum_door_keuringsinstantie", "meld_tijd_door_keuringsinstantie"]

CHECKS = [
    ("vehicules",         ["kenteken"]),
    ("carburant",         ["kenteken"]),
    ("carburant",         ["kenteken", "brandstof_volgnummer"]),
    ("controles",         CLE_CONTROLE),
    ("defauts_constates", CLE_CONTROLE + ["gebrek_identificatie"]),
    ("ref_defauts",       ["gebrek_identificatie"]),
]

for table, cle in CHECKS:
    df = spark.read.parquet(f"data/bronze/{table}")
    total = df.count()
    uniques = df.select(*cle).distinct().count()
    statut = "UNIQUE ✅" if total == uniques else f"DOUBLONS ❌ ({total - uniques:,})"
    print(f"{table:18} | clé={'+'.join(c[:12] for c in cle):60} | {total:>12,} | {uniques:>12,} | {statut}")

spark.stop()
