{#- Physical sanity: min <= avg <= max. Any returned row is a failure. -#}
select sol, temp_min_c, temp_avg_c, temp_max_c
from {{ ref('fct_sol_weather') }}
where temp_min_c > temp_avg_c
   or temp_avg_c > temp_max_c
