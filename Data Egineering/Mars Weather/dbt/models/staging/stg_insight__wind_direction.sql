{#-
    One row per sol x 16 compass bins (a wind rose). The API omits bins that had no samples,
    so we cross join the full set of bins and fill the gaps with 0: a missing bin means
    "no wind from that direction", not "unknown".
-#}

with bins as (
    select unnest(range(0, 16)) as direction_bin
),

counts as (
    select
        p.sol,
        b.direction_bin,
        coalesce(try_cast(json_extract_string(p.sol_json, '$.WD."' || b.direction_bin || '".ct') as integer), 0) as sample_count
    from {{ ref('stg_insight__sol_payloads') }} as p
    cross join bins as b
)

select
    counts.sol,
    counts.direction_bin,
    compass.compass_point,
    compass.compass_degrees,
    counts.sample_count
from counts
inner join {{ ref('compass_bins') }} as compass using (direction_bin)
