-- Ne garde que la dernière tranche chargée par la collecte.
with dernier_chargement as (
    select *
    from {{ source('raw', 'persons') }}
    where load_date = (select max(load_date) from {{ source('raw', 'persons') }})
)

select
    id,
    upper(trim(name)) as name,
    score,
    case
        when score >= 90 then 'A'
        when score >= 80 then 'B'
        else 'C'
    end as grade
from dernier_chargement
