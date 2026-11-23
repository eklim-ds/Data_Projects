{#- Grain: one row per Martian day (sol). The trusted, de-duplicated weather fact. -#}

with readings as (
    select * from {{ ref('stg_insight__sol_readings') }}
)

select
    sol,
    cast(first_utc as date) as earth_date,
    first_utc,
    last_utc,
    season,
    northern_season,
    southern_season,
    month_ordinal,

    temp_avg_c,
    temp_min_c,
    temp_max_c,
    temp_max_c - temp_min_c as temp_range_c,

    wind_avg_ms,
    wind_min_ms,
    wind_max_ms,

    pressure_avg_pa,
    pressure_min_pa,
    pressure_max_pa,

    dominant_wind_point,
    dominant_wind_degrees,

    temp_hours_with_data,
    wind_hours_with_data,
    pressure_hours_with_data,
    wind_direction_hours_with_data,

    -- "Complete" = every sensor covered enough of the sol AND the headline stats exist.
    (
        temp_hours_with_data >= {{ var('min_sensor_hours') }}
        and wind_hours_with_data >= {{ var('min_sensor_hours') }}
        and pressure_hours_with_data >= {{ var('min_sensor_hours') }}
        and wind_direction_hours_with_data >= {{ var('min_sensor_hours') }}
        and temp_avg_c is not null
        and wind_avg_ms is not null
        and wind_max_ms is not null
        and pressure_avg_pa is not null
    ) as is_complete,

    times_ingested,
    last_ingested_at
from readings
