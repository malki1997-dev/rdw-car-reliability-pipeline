"""Transformations Silver du projet RDW.

Fonctions pures : elles reçoivent des DataFrames et renvoient des DataFrames,
sans lire ni écrire de fichiers. On peut donc les tester avec de petites données.
"""
from pyspark.sql import DataFrame, Window, functions as F

CLE_CONTROLE = ["plate", "approval_type", "inspection_date", "inspection_time"]


# ---------- Fonctions utilitaires ----------

def to_date_yyyymmdd(col_name):
    """'20150312' -> date ; NULL si invalide (ex. valeur sentinelle '0')."""
    return F.to_date(F.try_to_timestamp(F.col(col_name), F.lit("yyyyMMdd")))


def to_bool_ja_nee(col_name):
    """'Ja' -> True, 'Nee' -> False, autre -> NULL."""
    return F.when(F.col(col_name) == "Ja", True).when(F.col(col_name) == "Nee", False)


def deduplicate_latest(df: DataFrame, key: str, order_col: str) -> DataFrame:
    """Garde une ligne par clé : celle avec la plus grande valeur de order_col."""
    w = Window.partitionBy(key).orderBy(F.desc(order_col))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def clean_model(df: DataFrame) -> DataFrame:
    """Retire la marque au début du modèle, même répétée (SAAB SAAB 9-3 -> 9-3)."""
    prefixe = F.concat(F.col("brand"), F.lit(" "))
    for _ in range(2):
        df = df.withColumn(
            "model",
            F.when(F.col("model").startswith(prefixe),
                   F.expr("substring(model, length(brand) + 2)"))
             .otherwise(F.col("model")))
    return df


# ---------- Tables Silver ----------

def clean_vehicles(bronze: DataFrame) -> DataFrame:
    df = bronze.select(
        F.col("kenteken").alias("plate"),
        F.col("voertuigsoort").alias("vehicle_type"),
        F.col("merk").alias("brand"),
        F.col("handelsbenaming").alias("model"),
        F.col("inrichting").alias("body_type"),
        to_date_yyyymmdd("datum_eerste_toelating").alias("first_admission_date"),
        to_date_yyyymmdd("datum_eerste_tenaamstelling_in_nederland").alias("first_registration_nl_date"),
        to_date_yyyymmdd("datum_tenaamstelling").alias("last_owner_change_date"),
        F.expr("try_cast(catalogusprijs AS INT)").alias("catalog_price"),
        to_bool_ja_nee("export_indicator").alias("is_exported"),
        to_bool_ja_nee("openstaande_terugroepactie_indicator").alias("has_open_recall"),
        to_bool_ja_nee("taxi_indicator").alias("is_taxi"),
        "_source_file",
        "ingestion_date",
    )
    df = df.withColumn("_page", F.regexp_extract("_source_file", r"part-(\d+)", 1).cast("int"))
    df = deduplicate_latest(df, "plate", "_page").drop("_page")
    return clean_model(df)


def build_fuel_type(bronze: DataFrame) -> DataFrame:
    """1 ligne par véhicule avec un type de carburant (règles ordonnées)."""
    par_vehicule = bronze.groupBy(F.col("kenteken").alias("plate")).agg(
        F.array_sort(F.collect_set("brandstof_omschrijving")).alias("fuels"),
        F.max("klasse_hybride_elektrisch_voertuig").alias("hybrid_class"))

    def has(carburant):
        return F.array_contains("fuels", carburant)

    fuel_type = (
        F.when(has("Waterstof"), "Hydrogen")
         .when(has("Elektriciteit") & (F.size("fuels") > 1), "Hybrid")
         .when(has("Elektriciteit"), "Electric")
         .when(has("LPG"), "LPG")
         .when((F.size("fuels") == 1) & has("Benzine"), "Petrol")
         .when((F.size("fuels") == 1) & has("Diesel"), "Diesel")
         .otherwise("Other"))

    return par_vehicule.select(
        "plate",
        fuel_type.alias("fuel_type"),
        F.concat_ws(" + ", "fuels").alias("fuel_list"),
        "hybrid_class")


def clean_controles(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.col("kenteken").alias("plate"),
        F.col("soort_erkenning_keuringsinstantie").alias("approval_type"),
        to_date_yyyymmdd("meld_datum_door_keuringsinstantie").alias("inspection_date"),
        F.col("meld_tijd_door_keuringsinstantie").alias("inspection_time"),
        F.col("soort_erkenning_omschrijving").alias("approval_description"),
        F.col("soort_melding_ki_omschrijving").alias("report_type"),
        to_date_yyyymmdd("vervaldatum_keuring").alias("expiry_date"))


def clean_defauts(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.col("kenteken").alias("plate"),
        F.col("soort_erkenning_keuringsinstantie").alias("approval_type"),
        to_date_yyyymmdd("meld_datum_door_keuringsinstantie").alias("inspection_date"),
        F.col("meld_tijd_door_keuringsinstantie").alias("inspection_time"),
        F.col("gebrek_identificatie").alias("defect_code"),
        F.expr("try_cast(aantal_gebreken_geconstateerd AS INT)").alias("defect_count"))


def clean_ref(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.col("gebrek_identificatie").alias("defect_code"),
        to_date_yyyymmdd("ingangsdatum_gebrek").alias("valid_from"),
        to_date_yyyymmdd("einddatum_gebrek").alias("valid_to"),
        F.col("gebrek_paragraaf_nummer").alias("paragraph_number"),
        F.col("gebrek_artikel_nummer").alias("article_number"),
        F.col("gebrek_omschrijving").alias("defect_description"))


def enrich_inspections(controles, defauts, vehicules, carburant) -> DataFrame:
    """Table enrichie : 1 ligne par contrôle avec défauts, véhicule, carburant et âge."""
    defauts_par_controle = defauts.groupBy(*CLE_CONTROLE).agg(
        F.count("*").alias("nb_defect_types"),
        F.sum("defect_count").alias("nb_defects"))

    v = vehicules.select("plate", "vehicle_type", "brand", "model", "first_admission_date")
    c = carburant.select("plate", "fuel_type")

    return (controles
            .join(defauts_par_controle, CLE_CONTROLE, "left")
            .fillna({"nb_defect_types": 0, "nb_defects": 0})
            .join(v, "plate", "left")
            .join(c, "plate", "left")
            .fillna({"fuel_type": "None"})
            .withColumn("vehicle_age_years",
                        F.round(F.months_between("inspection_date", "first_admission_date") / 12, 1))
            .withColumn("is_apk", F.col("approval_description").startswith("APK"))
            .withColumn("has_defect", F.col("nb_defects") > 0))
