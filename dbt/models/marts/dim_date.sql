-- 1 ligne par jour (calendrier)
select
    to_char(d, 'YYYYMMDD')::int          as date_key,
    d::date                              as full_date,
    extract(year from d)::int            as year,
    extract(quarter from d)::int         as quarter,
    extract(month from d)::int           as month,
    extract(isodow from d)::int          as day_of_week,
    extract(isodow from d) in (6, 7)     as is_weekend
from generate_series('2000-01-01'::date, '2030-12-31'::date, interval '1 day') as d
