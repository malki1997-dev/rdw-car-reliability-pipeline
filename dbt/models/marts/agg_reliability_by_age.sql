{{ config(grants={"select": ["rdw_api"]}) }}
-- Grain : 1 ligne = 1 marque / modèle / carburant / âge en années entières (voitures particulières, APK)
-- Seuil : au moins 50 contrôles par ligne pour que le taux soit interprétable.
select
    m.brand,
    m.model,
    m.fuel_type,
    floor(f.vehicle_age_years)::int                                   as age_years,
    count(*)                                                          as nb_inspections,
    round(100.0 * avg(case when f.has_defect then 1 else 0 end), 1)   as defect_rate_pct,
    round(avg(f.nb_defects)::numeric, 2)                              as avg_defects
from {{ ref('fact_inspection') }} f
join {{ ref('dim_model') }} m on f.model_key = m.model_key
where m.vehicle_type = 'Personenauto'
  and f.vehicle_age_years between 0 and 30
group by m.brand, m.model, m.fuel_type, floor(f.vehicle_age_years)::int
having count(*) >= 50
