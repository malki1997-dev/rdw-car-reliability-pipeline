-- Règle : le nombre total de défauts est >= au nombre de types de défauts, et jamais négatif
select inspection_id, nb_defects, nb_defect_types
from {{ ref('fact_inspection') }}
where nb_defects < 0
   or nb_defects < nb_defect_types
