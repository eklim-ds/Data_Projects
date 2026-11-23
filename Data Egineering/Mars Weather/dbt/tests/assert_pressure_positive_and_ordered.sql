{#- Physical sanity: pressure is positive and min <= avg <= max. -#}
select sol, pressure_min_pa, pressure_avg_pa, pressure_max_pa
from {{ ref('fct_sol_weather') }}
where pressure_min_pa <= 0
   or pressure_min_pa > pressure_avg_pa
   or pressure_avg_pa > pressure_max_pa
