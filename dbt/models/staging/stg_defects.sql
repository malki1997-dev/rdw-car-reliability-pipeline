-- 1 ligne par type de défaut constaté lors d'un contrôle
select
    md5(concat_ws('|', plate, approval_type, inspection_date::text, inspection_time)) as inspection_id,
    plate,
    inspection_date,
    defect_code,
    defect_count
from {{ source('raw', 'defauts_constates') }}
