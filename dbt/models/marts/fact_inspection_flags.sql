-- Grain : 1 ligne = 1 contrôle APK ayant au moins un défaut
-- Famille : seed defect_family (40 codes classés, les autres sont « non_classe »)
select
    d.inspection_id,
    bool_or(coalesce(fam.family, 'non_classe') <> 'entretien') as has_non_maintenance_defect,
    bool_or(coalesce(fam.family, 'non_classe') = 'technique')  as has_technical_defect,
    bool_or(coalesce(fam.family, 'non_classe') = 'entretien')  as has_maintenance_defect
from {{ ref('fact_defect') }} d
left join {{ ref('defect_family') }} fam on d.defect_code = fam.defect_code
group by d.inspection_id
