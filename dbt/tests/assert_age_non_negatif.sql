-- Règle : un véhicule ne peut pas être contrôlé avant sa 1re mise en circulation.
-- 9 cas connus (J5) : avertissement si > 0, erreur seulement si > 100.
{{ config(warn_if='>0', error_if='>100') }}

select inspection_id, vehicle_age_years
from {{ ref('fact_inspection') }}
where vehicle_age_years < 0
