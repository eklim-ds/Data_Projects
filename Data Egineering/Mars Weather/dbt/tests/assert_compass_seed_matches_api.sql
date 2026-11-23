{#-
    Guards our one assumption about the wind-direction histogram: the reference seed maps each
    compass point to the same degrees the API reports for its most common direction.
-#}
select
    w.sol,
    w.dominant_wind_point,
    w.dominant_wind_degrees,
    c.compass_degrees as seed_degrees
from {{ ref('fct_sol_weather') }} as w
left join {{ ref('compass_bins') }} as c on c.compass_point = w.dominant_wind_point
where w.dominant_wind_point is not null
  and (c.compass_point is null or abs(c.compass_degrees - w.dominant_wind_degrees) > 0.01)
