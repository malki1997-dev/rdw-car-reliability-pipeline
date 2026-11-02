{{ config(grants={"select": ["rdw_bi"]}) }}
-- Grain : 1 ligne = 1 marque / âge en années entières (voitures particulières, contrôles APK)
-- On garde des comptes (pas seulement des %) pour calculer intervalles de confiance et standardisation.
select
    m.brand,
    floor(f.vehicle_age_years)::int                                                  as age_years,
    count(*)                                                                         as nb_inspections,
    sum(case when f.has_defect then 1 else 0 end)                                    as nb_with_defect,
    sum(case when coalesce(fl.has_non_maintenance_defect, false) then 1 else 0 end)  as nb_with_non_maintenance_defect,
    sum(case when coalesce(fl.has_technical_defect, false) then 1 else 0 end)        as nb_with_technical_defect
from {{ ref('fact_inspection') }} f
join {{ ref('dim_model') }} m on f.model_key = m.model_key
left join {{ ref('fact_inspection_flags') }} fl on f.inspection_id = fl.inspection_id
where m.vehicle_type = 'Personenauto'
  and m.brand <> 'UNKNOWN'
  and f.vehicle_age_years between 0 and 30
group by m.brand, floor(f.vehicle_age_years)::int
