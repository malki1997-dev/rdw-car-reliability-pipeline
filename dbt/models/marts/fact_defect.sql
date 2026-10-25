-- Grain : 1 ligne = 1 type de défaut constaté dans 1 contrôle APK
select
    d.inspection_id,
    d.defect_code,          -- -> dim_defect
    i.date_key,             -- -> dim_date
    i.model_key,            -- -> dim_model
    d.defect_count          -- mesure
from {{ ref('stg_defects') }} d
inner join {{ ref('stg_inspections') }} i
    on d.inspection_id = i.inspection_id   -- seulement les contrôles APK connus
