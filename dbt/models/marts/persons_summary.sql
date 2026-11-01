select
    grade,
    count(*) as person_count,
    round(avg(score), 2) as avg_score,
    min(score) as min_score,
    max(score) as max_score
from {{ ref('stg_persons') }}
group by grade
