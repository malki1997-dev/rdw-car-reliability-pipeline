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
