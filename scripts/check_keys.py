import pandas as pd

def check(nom, fichier, cle):
    # dtype=str : on garde les valeurs brutes, sans conversion automatique
    df = pd.read_csv(f"data/landing/{fichier}", dtype=str)
    total = len(df)
    uniques = len(df.drop_duplicates(subset=cle))
    statut = "UNIQUE ✅" if total == uniques else "DOUBLONS ❌"
    print(f"{nom:28} | lignes={total:5} | clés uniques={uniques:5} | {statut}")

CLE_CONTROLE = ["kenteken", "soort_erkenning_keuringsinstantie",
                "meld_datum_door_keuringsinstantie", "meld_tijd_door_keuringsinstantie"]

check("Véhicules (kenteken)",       "sample_vehicules.csv",         ["kenteken"])
check("Carburant (kenteken)",       "sample_carburant.csv",         ["kenteken"])
check("Carburant (kenteken + num)", "sample_carburant.csv",         ["kenteken", "brandstof_volgnummer"])
check("Contrôles (clé composite)",  "sample_controles.csv",         CLE_CONTROLE)
check("Défauts (clé + code)",       "sample_defauts_constates.csv", CLE_CONTROLE + ["gebrek_identificatie"])
check("Référentiel (code)",         "sample_ref_defauts.csv",       ["gebrek_identificatie"])
