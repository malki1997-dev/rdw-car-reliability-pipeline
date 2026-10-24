import datetime
from pyspark.sql import functions as F
from rdw import transforms as t


def test_date_valide_et_valeur_sentinelle(spark):
    df = spark.createDataFrame([("20150312",), ("0",)], ["d"])
    res = [r[0] for r in df.select(t.to_date_yyyymmdd("d")).collect()]
    assert res == [datetime.date(2015, 3, 12), None]   # "0" -> NULL


def test_clean_model_retire_la_marque_meme_repetee(spark):
    df = spark.createDataFrame(
        [("NISSAN", "NISSAN QASHQAI"), ("SAAB", "SAAB SAAB 9-3"), ("FORD", "PUMA")],
        ["brand", "model"])
    res = [r.model for r in t.clean_model(df).collect()]
    assert res == ["QASHQAI", "9-3", "PUMA"]


def test_deduplicate_garde_la_version_la_plus_recente(spark):
    df = spark.createDataFrame(
        [("AA111", 279, "ancien"), ("AA111", 280, "recent"), ("BB222", 10, "unique")],
        ["plate", "page", "valeur"])
    res = {r.plate: r.valeur for r in t.deduplicate_latest(df, "plate", "page").collect()}
    assert res == {"AA111": "recent", "BB222": "unique"}


def test_fuel_type_regles(spark):
    lignes = [
        ("P1", "Benzine", None),
        ("H1", "Benzine", None), ("H1", "Elektriciteit", None),
        ("E1", "Elektriciteit", None),
        ("W1", "Elektriciteit", None), ("W1", "Waterstof", None),
        ("L1", "Benzine", None), ("L1", "LPG", None),
    ]
    bronze = spark.createDataFrame(
        lignes, "kenteken string, brandstof_omschrijving string, klasse_hybride_elektrisch_voertuig string")
    res = {r.plate: r.fuel_type for r in t.build_fuel_type(bronze).collect()}
    assert res == {"P1": "Petrol", "H1": "Hybrid", "E1": "Electric", "W1": "Hydrogen", "L1": "LPG"}


def test_enrichissement_ne_multiplie_pas_les_controles(spark):
    d = datetime.date(2025, 1, 10)
    controles = spark.createDataFrame(
        [("AA111", "AL", d, "1000", "APK Lichte voertuigen"),
         ("BB222", "AL", d, "1100", "APK Lichte voertuigen")],
        "plate string, approval_type string, inspection_date date, inspection_time string, approval_description string")
    # AA111 : 2 types de défauts ; BB222 : aucun défaut
    defauts = spark.createDataFrame(
        [("AA111", "AL", d, "1000", "205", 1), ("AA111", "AL", d, "1000", "210", 2)],
        "plate string, approval_type string, inspection_date date, inspection_time string, defect_code string, defect_count int")
    vehicules = spark.createDataFrame(
        [("AA111", "Personenauto", "TOYOTA", "YARIS", datetime.date(2015, 1, 10)),
         ("BB222", "Personenauto", "KIA", "PICANTO", datetime.date(2020, 1, 10))],
        "plate string, vehicle_type string, brand string, model string, first_admission_date date")
    carburant = spark.createDataFrame([("AA111", "Hybrid")], "plate string, fuel_type string")

    res = {r.plate: r for r in t.enrich_inspections(controles, defauts, vehicules, carburant).collect()}

    assert len(res) == 2                       # pas de multiplication
    assert res["AA111"].nb_defects == 3        # 1 + 2
    assert res["BB222"].nb_defects == 0        # left join + fillna
    assert res["BB222"].fuel_type == "None"    # pas de carburant connu
    assert res["AA111"].vehicle_age_years == 10.0
