from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[*]").appName("j5_joins")
         .config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

defauts = spark.read.parquet("data/silver/defauts_constates")
ref = spark.read.parquet("data/silver/ref_defauts").select("defect_code", "defect_description")

# Broadcast join : la petite table (1 002 lignes) est copiée sur chaque partition
joined = defauts.join(F.broadcast(ref), "defect_code", "left")

print(f"Défauts avant jointure : {defauts.count():,}")
print(f"Défauts après jointure : {joined.count():,}")

# Anti join : défauts dont le code n'existe pas dans le référentiel
orphelins = defauts.join(ref, "defect_code", "left_anti")
print(f"Codes orphelins        : {orphelins.count():,}")

# Top 10 des défauts les plus fréquents
(joined.groupBy("defect_code", "defect_description")
       .agg(F.sum("defect_count").alias("total"))
       .orderBy(F.desc("total"))
       .show(10, truncate=False))

joined.explain()
spark.stop()
