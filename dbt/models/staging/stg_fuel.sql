-- 1 ligne par véhicule
select
    plate,
    fuel_type,
    hybrid_class
from {{ source('raw', 'carburant') }}
