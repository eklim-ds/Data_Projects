{#- One typed row per sol. Units follow the API: temperature °C, wind m/s, pressure Pa. -#}

select
    sol,
    source_file,
    ingested_at as last_ingested_at,
    times_ingested,

    {{ json_utc_timestamp('sol_json', '$.First_UTC') }} as first_utc,
    {{ json_utc_timestamp('sol_json', '$.Last_UTC') }} as last_utc,
    json_extract_string(sol_json, '$.Season') as season,
    json_extract_string(sol_json, '$.Northern_season') as northern_season,
    json_extract_string(sol_json, '$.Southern_season') as southern_season,
    {{ json_int('sol_json', '$.Month_ordinal') }} as month_ordinal,

    {{ json_double('sol_json', '$.AT.av') }} as temp_avg_c,
    {{ json_double('sol_json', '$.AT.mn') }} as temp_min_c,
    {{ json_double('sol_json', '$.AT.mx') }} as temp_max_c,
    {{ json_int('sol_json', '$.AT.ct') }} as temp_sample_count,

    {{ json_double('sol_json', '$.HWS.av') }} as wind_avg_ms,
    {{ json_double('sol_json', '$.HWS.mn') }} as wind_min_ms,
    {{ json_double('sol_json', '$.HWS.mx') }} as wind_max_ms,
    {{ json_int('sol_json', '$.HWS.ct') }} as wind_sample_count,

    {{ json_double('sol_json', '$.PRE.av') }} as pressure_avg_pa,
    {{ json_double('sol_json', '$.PRE.mn') }} as pressure_min_pa,
    {{ json_double('sol_json', '$.PRE.mx') }} as pressure_max_pa,
    {{ json_int('sol_json', '$.PRE.ct') }} as pressure_sample_count,

    json_extract_string(sol_json, '$.WD.most_common.compass_point') as dominant_wind_point,
    {{ json_double('sol_json', '$.WD.most_common.compass_degrees') }} as dominant_wind_degrees,
    {{ json_int('sol_json', '$.WD.most_common.ct') }} as dominant_wind_sample_count,

    -- Sensor coverage: how many of the 24 sol-hours had data (from the API's validity_checks).
    coalesce(json_array_length(validity_json, '$.AT.sol_hours_with_data'), 0) as temp_hours_with_data,
    coalesce(json_array_length(validity_json, '$.HWS.sol_hours_with_data'), 0) as wind_hours_with_data,
    coalesce(json_array_length(validity_json, '$.PRE.sol_hours_with_data'), 0) as pressure_hours_with_data,
    coalesce(json_array_length(validity_json, '$.WD.sol_hours_with_data'), 0) as wind_direction_hours_with_data
from {{ ref('stg_insight__sol_payloads') }}
