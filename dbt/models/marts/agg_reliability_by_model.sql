{{ config(grants={"select": ["rdw_api"]}) }}
-- Grain : 1 ligne = 1 combinaison marque / modèle / carburant (voitures particulières, APK)
-- Seuil : au moins 100 contrôles, sinon le taux de défaut n'est pas fiable statistiquement.
select
    m.brand,
    m.model,
    m.fuel_type,
    count(*)                                                          as nb_inspections,
    sum(case when f.has_defect then 1 else 0 end)                     as nb_with_defect,
    round(100.0 * avg(case when f.has_defect then 1 else 0 end), 1)   as defect_rate_pct,
    round(avg(f.vehicle_age_years)::numeric, 1)                       as avg_age_years,
    round(avg(f.nb_defects)::numeric, 2)                              as avg_defects
from {{ ref('fact_inspection') }} f
join {{ ref('dim_model') }} m on f.model_key = m.model_key
where m.vehicle_type = 'Personenauto'
group by m.brand, m.model, m.fuel_type
having count(*) >= 100
