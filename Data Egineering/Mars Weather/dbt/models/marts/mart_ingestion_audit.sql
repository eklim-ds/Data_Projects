{#-
    Pipeline health, one row per batch run (landed file): how many sols did it return, and how
    many of those were new vs. already seen? Because the API returns an overlapping rolling
    window, a healthy daily run usually shows a handful of new sols and a few repeats. A run with
    zero new sols is expected when the feed is paused or frozen, which is the case for the
    retired InSight mission.
-#}

with files as (
    select
        source_file,
        ingested_at,
        size_bytes,
        sol_keys,
        coalesce(len(sol_keys), 0) as sols_returned,
        list_min(sol_keys) as min_sol,
        list_max(sol_keys) as max_sol
    from {{ ref('stg_insight__ingestion_files') }}
),

exploded as (
    select source_file, unnest(sol_keys) as sol
    from files
),

first_seen as (
    select
        e.sol,
        min(f.ingested_at) as first_ingested_at
    from exploded as e
    inner join files as f using (source_file)
    group by e.sol
),

per_file as (
    select
        f.source_file,
        f.ingested_at,
        f.size_bytes,
        f.sols_returned,
        f.min_sol,
        f.max_sol,
        cast(coalesce(sum(case when fs.first_ingested_at = f.ingested_at then 1 else 0 end), 0) as integer) as new_sols
    from files as f
    left join exploded as e using (source_file)
    left join first_seen as fs on fs.sol = e.sol
    group by all
)

select
    source_file,
    ingested_at,
    size_bytes,
    sols_returned,
    min_sol,
    max_sol,
    new_sols,
    sols_returned - new_sols as repeated_sols,
    cast(sum(new_sols) over (order by ingested_at) as integer) as cumulative_distinct_sols
from per_file
