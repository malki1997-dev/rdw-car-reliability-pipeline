from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[*]").appName("j5_controles_defauts")
         .config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

CLE = ["plate", "approval_type", "inspection_date", "inspection_time"]

controles = spark.read.parquet("data/silver/controles")
defauts = spark.read.parquet("data/silver/defauts_constates")

# 1. Agréger : 1 ligne par contrôle
defauts_par_controle = defauts.groupBy(*CLE).agg(
    F.count("*").alias("nb_defect_types"),
    F.sum("defect_count").alias("nb_defects"),
)

# 2. Left join : on garde tous les contrôles, même sans défaut
res = (controles.join(defauts_par_controle, CLE, "left")
       .fillna({"nb_defect_types": 0, "nb_defects": 0}))

# Contrôles
print(f"Contrôles avant : {controles.count():,}")
print(f"Contrôles après : {res.count():,}")
avec = res.filter(F.col("nb_defects") > 0).count()
print(f"Contrôles avec au moins 1 défaut : {avec:,} ({100 * avec / res.count():.1f} %)")

# Défauts sans contrôle correspondant (orphelins)
orphelins = defauts_par_controle.join(controles, CLE, "left_anti").count()
print(f"Groupes de défauts sans contrôle : {orphelins:,}")

spark.stop()
