{#- Grain: sol x compass bin (16 rows per sol). `share_of_samples` makes sols with different sample counts comparable. -#}

select
    sol,
    direction_bin,
    compass_point,
    compass_degrees,
    sample_count,
    case
        when sum(sample_count) over (partition by sol) > 0
            then sample_count * 1.0 / sum(sample_count) over (partition by sol)
        else 0.0
    end as share_of_samples
from {{ ref('stg_insight__wind_direction') }}
