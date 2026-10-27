-- Dimension historisée (SCD2) : 1 ligne par version de véhicule
with snap as (
    select
        *,
        row_number() over (partition by plate order by dbt_valid_from) as version
    from {{ ref('snap_vehicles') }}
)
select
    dbt_scd_id                                     as vehicle_key,
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
    is_taxi,
    version,
    -- la 1re version couvre tout le passé
    case when version = 1 then timestamp '1900-01-01' else dbt_valid_from end as valid_from,
    coalesce(dbt_valid_to, timestamp '9999-12-31') as valid_to,
    dbt_valid_to is null                           as is_current
from snap
