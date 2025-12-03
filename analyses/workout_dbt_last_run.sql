{#
  Filter workouts from the last dbt run, using the dbt_run_started_at
  column you already have in workout_metrics.
#}

{% set runs_back = var('runs_back', 1) %}

with runs as (
    select
      dbt_run_started_at,
      dense_rank() over (order by dbt_run_started_at desc) as run_rank
    from {{ ref('workout') }}
    group by 1
),

chosen_run as (
    select dbt_run_started_at
    from runs
    where run_rank = {{ runs_back }}
),

filtered as (
    select wm.*
    from {{ ref('workout') }} wm
    join chosen_run r
      on wm.dbt_run_started_at = r.dbt_run_started_at
)

select *
from filtered
order by workout_timestamp desc;{#
  Filter workouts from the last dbt run, using the dbt_run_started_at
  column you already have in workout_metrics.
#}

{% set runs_back = var('runs_back', 1) %}

with runs as (
    select
      dbt_run_started_at,
      dense_rank() over (order by dbt_run_started_at desc) as run_rank
    from {{ ref('workout') }}
    group by 1
),

chosen_run as (
    select dbt_run_started_at
    from runs
    where run_rank = {{ runs_back }}
),

filtered as (
    select wm.*
    from {{ ref('workout') }} wm
    join chosen_run r
      on wm.dbt_run_started_at = r.dbt_run_started_at
)

select *
from filtered
order by workout_timestamp desc;