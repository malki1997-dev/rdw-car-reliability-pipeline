-- 1 ligne par véhicule
select
    plate,
    vehicle_type,
    brand,
    model,
    body_type,
    first_admission_date,
    last_owner_change_date,
    catalog_price,
    is_exported,
    has_open_recall,
    is_taxi
from {{ source('raw', 'vehicules') }}
