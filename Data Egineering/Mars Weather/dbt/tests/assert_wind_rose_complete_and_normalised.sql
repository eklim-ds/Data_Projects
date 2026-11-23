{#-
    Every sol must have exactly 16 bins (one row per sol x bin), and the shares of a sol that
    has any wind samples must sum to 1. Any returned row is a failure.
-#}
select
    sol,
    count(*) as bins,
    count(distinct direction_bin) as distinct_bins,
    sum(share_of_samples) as total_share
from {{ ref('fct_sol_wind_rose') }}
group by sol
having count(*) <> 16
    or count(distinct direction_bin) <> 16
    or (sum(sample_count) > 0 and abs(sum(share_of_samples) - 1.0) > 1e-9)
