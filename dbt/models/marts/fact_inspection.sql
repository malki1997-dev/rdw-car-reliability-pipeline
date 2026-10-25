-- Grain : 1 ligne = 1 contrôle technique APK
select
    inspection_id,          -- identifiant du contrôle (dimension dégénérée)
    date_key,               -- -> dim_date
    model_key,              -- -> dim_model
    plate,                  -- -> dim_vehicule (J8)
    vehicle_age_years,      -- mesure
    nb_defect_types,        -- mesure
    nb_defects,             -- mesure
    has_defect              -- indicateur
from {{ ref('stg_inspections') }}
