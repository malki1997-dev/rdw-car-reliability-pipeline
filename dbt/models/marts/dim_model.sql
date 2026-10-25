-- 1 ligne par combinaison marque / modèle / type de véhicule / carburant
select distinct
    model_key,
    brand,
    model,
    vehicle_type,
    fuel_type
from {{ ref('stg_inspections') }}
