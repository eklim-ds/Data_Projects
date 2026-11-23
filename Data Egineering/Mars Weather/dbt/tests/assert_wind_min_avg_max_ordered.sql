{#- Physical sanity: wind speed cannot be negative and min <= avg <= max. -#}
select sol, wind_min_ms, wind_avg_ms, wind_max_ms
from {{ ref('fct_sol_weather') }}
where wind_min_ms < 0
   or wind_min_ms > wind_avg_ms
   or wind_avg_ms > wind_max_ms
