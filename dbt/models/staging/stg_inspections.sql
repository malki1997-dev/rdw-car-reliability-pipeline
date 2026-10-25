-- 1 ligne par contrôle technique (APK uniquement)
with base as (
    select
        md5(concat_ws('|', plate, approval_type, inspection_date::text, inspection_time)) as inspection_id,
        plate,
        inspection_date,
        approval_description,
        expiry_date,
        coalesce(brand, 'UNKNOWN')        as brand,
        coalesce(model, 'UNKNOWN')        as model,
        coalesce(vehicle_type, 'UNKNOWN') as vehicle_type,
        fuel_type,
        vehicle_age_years,
        nb_defect_types,
        nb_defects,
        has_defect
    from {{ source('raw', 'inspections_enriched') }}
    where is_apk
)
select
    *,
    md5(concat_ws('|', brand, model, vehicle_type, fuel_type)) as model_key,
    to_char(inspection_date, 'YYYYMMDD')::int                  as date_key
from base
