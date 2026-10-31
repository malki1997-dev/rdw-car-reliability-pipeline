# Sources de données — RDW Open Data

Données publiques du RDW (Pays-Bas), licence CC-0, sans clé ni inscription.

## Datasets

| Dataset | ID | Lignes | Colonnes | Mise à jour | Grain (1 ligne =) | Clé |
|---|---|---|---|---|---|---|
| Véhicules (Gekentekende voertuigen) | m9d7-ebf2 | 16,9 M | 98 | quotidienne | 1 véhicule | kenteken |
| Carburant (Brandstof) | 8ys7-d773 | 17,0 M | 36 | quotidienne | 1 carburant d'1 véhicule | kenteken + brandstof_volgnummer |
| Contrôles (Meldingen keuringsinstantie) | sgfe-77wx | 24,9 M | 11 | quotidienne | 1 contrôle | kenteken + soort_erkenning + meld_datum + meld_tijd |
| Défauts constatés (Geconstateerde gebreken) | a34c-vvps | 24,6 M | 8 | quotidienne | 1 type de défaut dans 1 contrôle | clé contrôle + gebrek_identificatie |
| Référentiel défauts (Gebreken) | hx2c-gt7k | 1 002 | 8 | ~mensuelle | 1 code de défaut | gebrek_identificatie |

## Relations

    VEHICULES --(kenteken)-- CONTROLES --(clé contrôle)-- DEFAUTS --(gebrek_identificatie)-- REFERENTIEL
        |
        +--(kenteken)-- CARBURANT  (1 à 2 lignes par véhicule)

## Colonnes retenues (NL → FR)

### Véhicules
| Colonne | Signification |
|---|---|
| kenteken | Plaque d'immatriculation |
| voertuigsoort | Type de véhicule |
| merk | Marque |
| handelsbenaming | Modèle (nom commercial) |
| inrichting | Carrosserie |
| datum_eerste_toelating | Date de 1re mise en circulation (calcul de l'âge) |
| datum_eerste_tenaamstelling_in_nederland | Date de 1re immatriculation aux Pays-Bas |
| datum_tenaamstelling | Date du dernier changement de propriétaire |
| catalogusprijs | Prix catalogue |
| export_indicator | Véhicule exporté |
| openstaande_terugroepactie_indicator | Rappel constructeur en cours |
| taxi_indicator | Taxi |

### Carburant
| Colonne | Signification |
|---|---|
| brandstof_volgnummer | N° d'ordre du carburant (hybride = 1 et 2) |
| brandstof_omschrijving | Carburant (Benzine, Diesel, Elektriciteit, LPG...) |
| klasse_hybride_elektrisch_voertuig | Classe hybride |

### Contrôles
| Colonne | Signification |
|---|---|
| soort_erkenning_keuringsinstantie | Type d'agrément de l'organisme |
| meld_datum_door_keuringsinstantie | Date du contrôle (yyyyMMdd) |
| meld_tijd_door_keuringsinstantie | Heure du contrôle |
| vervaldatum_keuring | Date d'expiration du contrôle |

### Défauts constatés
| Colonne | Signification |
|---|---|
| gebrek_identificatie | Code du défaut |
| aantal_gebreken_geconstateerd | Nombre de fois où le défaut est constaté |

### Référentiel
| Colonne | Signification |
|---|---|
| gebrek_omschrijving | Description du défaut |
| ingangsdatum_gebrek / einddatum_gebrek | Dates de validité du code |

## Points d'attention

- **Carburant** : un véhicule hybride a 2 lignes. Une jointure directe sur kenteken duplique les contrôles → regrouper à 1 ligne par véhicule (type_carburant) avant la jointure.
- **Pas d'identifiant de contrôle** : clé composite de 4 colonnes.
- **Colonnes _dt** : doublons typés des dates, une seule version à garder.
- **Fréquence du pipeline** : mensuelle (besoin métier), même si la source est quotidienne.
- **Validation** : clés vérifiées sur échantillon (scripts/check_keys.py), à revalider sur le volume complet avec Spark.

## Qualité des données — constats J3

- **Volume complet ingéré** : 83,5 M lignes (CSV 5,9 Go → Parquet Bronze 705 Mo, ~8× plus petit, lecture ~7× plus rapide).
- **Clés validées sur le volume complet** : Contrôles, Défauts, Référentiel et Carburant (kenteken + volgnummer) uniques.
- **Véhicules : 56 183 plaques en double**, dont 56 125 strictement identiques, toutes issues des pages 279 et 280.
  Cause : dérive de la pagination par offset pendant une mise à jour de la source (suppression de lignes en amont).
  Traitement : Bronze conservé tel quel ; dédoublonnage en Silver (version de la page la plus récente).
  Correctif prévu : keyset pagination (kenteken > dernière valeur) au lieu de $offset.

## Qualité des données — constats J3

- **Volume complet ingéré** : 83,5 M lignes (CSV 5,9 Go → Parquet Bronze 705 Mo, ~8× plus petit, lecture ~7× plus rapide).
- **Clés validées sur le volume complet** : Contrôles, Défauts, Référentiel et Carburant (kenteken + volgnummer) uniques.
- **Véhicules : 56 183 plaques en double**, dont 56 125 strictement identiques, toutes issues des pages 279 et 280.
  Cause : dérive de la pagination par offset pendant une mise à jour de la source (suppression de lignes en amont).
  Traitement : Bronze conservé tel quel ; dédoublonnage en Silver (version de la page la plus récente).
  Correctif prévu : keyset pagination (kenteken > dernière valeur) au lieu de $offset.

## Couche Silver — règles de nettoyage (J4)

- **Colonnes renommées** en anglais snake_case (kenteken → plate, merk → brand...).
- **Typage** : dates yyyyMMdd → date, prix → integer, Ja/Nee → boolean. Fonctions try_ : une valeur invalide devient NULL au lieu de bloquer le pipeline (mode ANSI de Spark 4).
- **Contrôle avant/après** : on compare les NULL de Bronze et de Silver pour détecter les conversions ratées.
- **Véhicules** : 56 183 doublons supprimés (window function row_number, version de la page la plus récente) → 16 813 284 véhicules uniques.
- **Modèle** : la marque est retirée du début du nom, même répétée (NISSAN QASHQAI → QASHQAI, SAAB SAAB 9-3 → 9-3). 2,3 M modèles nettoyés.
- **catalog_price** : 38,9 % NULL dans la source ; 5 valeurs "Nee" (remorques) converties en NULL.
- **Carburant** : 1 ligne par véhicule (15 162 676) avec fuel_type = Petrol, Diesel, Hybrid, Electric, LPG, Hydrogen, Other. Règles ordonnées (Waterstof testé avant Elektriciteit). fuel_list garde la combinaison d'origine.
- **expiry_date** : la valeur sentinelle "0" de la source (657 069 contrôles) est convertie en NULL.
- **Pas de filtre métier en Silver** : tous les types de véhicules sont conservés ; les filtres (ex. voitures particulières) seront appliqués en Gold.

## Jointures et table enrichie (J5)

- **Défauts × Référentiel** : broadcast join (1 002 lignes), 0 code orphelin.
- **Contrôles × Défauts** : défauts agrégés à 1 ligne par contrôle avant le left join (pas de multiplication). 51,8 % des contrôles ont au moins un défaut.
- **3 521 groupes de défauts sans contrôle** (0,03 %) : hypothèse du décalage de téléchargement réfutée (dates anciennes) ; 829 viennent du seul 2025-06-02 → incohérence de la source. Exclus.
- **89 879 contrôles sans véhicule** (0,4 %) : véhicules absents du dataset Véhicules. Conservés avec marque NULL.
- **Âges négatifs (2 769)** : installations de tachygraphes ou de GPL avant immatriculation (médiane -0,1 an). Seulement 9 APK.
- **Le dataset Contrôles ne contient pas que des contrôles techniques** : 1,37 % sont des tachygraphes ou installations GPL. Colonne is_apk ajoutée ; Gold ne gardera que is_apk = true.
- **Biais d'âge** : les véhicules électriques, récents, ont peu de contrôles (1er APK après ~4 ans).
- **Plan d'exécution** : 3 SortMergeJoin, 5 shuffles ; la jointure Carburant réutilise le partitionnement par plate.
- Table produite : data/silver/inspections_enriched (24 952 595 lignes).

## Industrialisation et chargement PostgreSQL (J6)

- **Module rdw/transforms.py** : toutes les transformations Silver en fonctions pures (DataFrame -> DataFrame).
- **Tests pytest** (5 tests) : valeur sentinelle, nettoyage du modèle, dédoublonnage, règles de carburant, absence de multiplication dans les jointures.
- **Pipeline unique** spark/pipeline_silver.py lancé avec spark-submit : 6 tables Silver en ~5 min.
- **Partitions** : AQE a réduit les 200 partitions de shuffle à 8-23 fichiers par table ; pas d'optimisation supplémentaire nécessaire à ce volume.
- **Chargement PostgreSQL** (spark/load_postgres.py) : JDBC, coalesce(4) sans shuffle, batchs de 10 000, table par table ; secrets dans .env.
- **Incident** : un premier essai (repartition(8), 4 Go de driver) a saturé la RAM de WSL (7,7 Go) et fait planter WSL. Corrigé par .wslconfig (10 Go + 8 Go de swap), coalesce et chargement table par table.
- **Volumes** : 870 Mo en Parquet contre 8,1 Go dans PostgreSQL (~9×), illustration du stockage colonne vs ligne.

## Couche Gold avec dbt (J7)

- **Projet dbt** dans dbt/ ; connexion via profiles.yml avec env_var (pas de mot de passe dans le code).
- **Sources** : 5 tables du schéma raw déclarées dans _sources.yml.
- **Staging** (vues, schéma staging) : stg_inspections (APK uniquement, inspection_id et model_key en md5, date_key), stg_defects, stg_vehicles, stg_fuel, stg_defect_ref.
- **Dimensions** (tables, schéma marts) : dim_model (115 570 combinaisons marque/modèle/type/carburant, surrogate key), dim_defect (1 002, clé naturelle), dim_date (calendrier 2000-2030).
- **Faits** : fact_inspection (24 612 617 contrôles APK), fact_defect (24 564 224 défauts, inner join sur les contrôles APK).
- **Première analyse** : Audi et BMW ont le plus faible taux de contrôles avec défaut (~42-43 %), mais la comparaison doit tenir compte de l'âge moyen (biais d'âge) et de l'entretien.

## Historique, tests et documentation dbt (J8)

- **Snapshot SCD2** (snapshots/snap_vehicles.yml) : stratégie check sur propriétaire, export, rappel et taxi. 1er snapshot : 16 813 284 lignes.
- **Simulation** d'un changement (3 véhicules passés à exportés dans raw) : dbt a fermé l'ancienne version et créé la nouvelle. Le check compare toutes les lignes (345 s) ; une stratégie timestamp serait plus rapide si la source avait une date de mise à jour.
- **dim_vehicle** (SCD2) : vehicle_key par version, valid_from (1re version depuis 1900-01-01), valid_to (version actuelle jusqu'à 9999-12-31), is_current. Permet la jointure point-in-time.
- **15 tests génériques** (unique, not_null, relationships, accepted_values) : tous PASS.
- **2 tests singuliers** : cohérence des défauts (PASS) ; âge non négatif en WARN (9 anomalies connues, erreur au-delà de 100).
- **Documentation et lineage** (dbt docs) : le graphe a révélé que stg_fuel n'est utilisé par aucun modèle aval.

## Orchestration avec Airflow (J9)

- **Airflow 3.3.1** dans Docker (mode standalone, LocalExecutor), base de métadonnées dans la base `airflow` du PostgreSQL du projet.
- **Image personnalisée** (airflow/Dockerfile) : Java 17, PySpark 4.2, dbt 1.12 dans un venv séparé pour éviter les conflits avec Airflow.
- **Connexion flexible** : PG_HOST / PG_PORT en variables d'environnement (localhost:5433 depuis le PC, rdw-postgres:5432 depuis Docker).
- **DAG rdw_pipeline** : download >> bronze >> silver >> load_postgres >> dbt_snapshot >> dbt_run >> dbt_test. schedule=@monthly (besoin métier), catchup=False, max_active_runs=1, retries=1.
- **INGESTION_DATE** passée en paramètre de run (défaut : date logique) : relancer un mois donné reproduit les mêmes données (idempotence), et le téléchargement saute les fichiers déjà complets.
- **load_postgres** en truncate au lieu de drop : les vues dbt dépendent des tables raw.
- **Exécution complète** : 7 tâches en 36 minutes, tests dbt PASS.
- **SCD2 de bout en bout** : les 3 véhicules modifiés manuellement au J8 ont bien obtenu une 3e version lors du rechargement, l'historique est intact.
- **Incidents** : (1) run « Success » sans tâche car la date logique précédait le start_date ; (2) script de téléchargement ignorant INGESTION_DATE (modification du J3 non appliquée), détecté par un téléchargement anormalement long ; run stoppé et corrigé.

## Tables résumées pour l'API (J10)

- **agg_reliability_by_model** : 1 ligne par marque / modèle / carburant (voitures particulières, APK), seuil de 100 contrôles. 3 887 lignes, 520 Ko.
- **agg_reliability_by_age** : même grain avec l'âge en années entières (0 à 30 ans), seuil de 50 contrôles. 16 130 lignes, 1,4 Mo.
- **Biais d'âge démontré** : la Toyota Yaris essence affiche 52,8 % de contrôles avec défaut contre 38,7 % pour la Yaris hybride, mais l'âge moyen est de 16,8 ans contre 9,2 ans. À âge égal (8, 9 et 10 ans), les taux sont quasi identiques (39 à 41 %).
- **Limite** : un même nom de modèle peut regrouper plusieurs générations.
- **Moindre privilège** : l'utilisateur PostgreSQL rdw_api n'a le droit de lire que ces deux tables (grants dbt réappliqués à chaque run). Son mot de passe est dans .env, ignoré par Git.
