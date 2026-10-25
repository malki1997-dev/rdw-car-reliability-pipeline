-- 1 ligne par code de défaut
select
    defect_code,
    defect_description,
    paragraph_number,
    article_number,
    valid_from,
    valid_to
from {{ source('raw', 'ref_defauts') }}
