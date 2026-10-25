-- 1 ligne par code de défaut (clé naturelle : defect_code)
select
    defect_code,
    defect_description,
    paragraph_number,
    article_number,
    valid_from,
    valid_to
from {{ ref('stg_defect_ref') }}
