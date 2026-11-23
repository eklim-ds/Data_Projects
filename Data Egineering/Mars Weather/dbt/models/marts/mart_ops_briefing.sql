{#-
    The dashboard's main table: the daily GO / CAUTION / NO_GO call for wind-sensitive work,
    with the reason in plain words, plus rolling context. Thresholds come from dbt vars
    (dbt_project.yml) so they can be tuned without touching SQL.
-#}

with weather as (
    select * from {{ ref('fct_sol_weather') }}
),

scored as (
    select
        *,
        case
            when not is_complete then 'NO_DATA'
            when wind_max_ms > {{ var('ops_nogo_gust_ms') }} then 'NO_GO'
            when wind_max_ms > {{ var('ops_caution_gust_ms') }} then 'CAUTION'
            when wind_avg_ms > {{ var('ops_caution_avg_wind_ms') }} then 'CAUTION'
            else 'GO'
        end as ops_status,

        case
            when not is_complete then 'Sensor coverage too low to judge this sol'
            when wind_max_ms > {{ var('ops_nogo_gust_ms') }}
                then 'Peak wind ' || round(wind_max_ms, 1) || ' m/s exceeds the ' || {{ var('ops_nogo_gust_ms') }} || ' m/s no-go limit'
            when wind_max_ms > {{ var('ops_caution_gust_ms') }}
                then 'Peak wind ' || round(wind_max_ms, 1) || ' m/s exceeds the ' || {{ var('ops_caution_gust_ms') }} || ' m/s caution limit'
            when wind_avg_ms > {{ var('ops_caution_avg_wind_ms') }}
                then 'Average wind ' || round(wind_avg_ms, 1) || ' m/s exceeds the ' || {{ var('ops_caution_avg_wind_ms') }} || ' m/s caution limit'
            else 'Wind within limits'
        end as ops_status_reason,

        -- Limits are exposed as columns so BI tools draw the same lines the rules use (one source of truth).
        {{ var('ops_caution_avg_wind_ms') }} as limit_caution_avg_wind_ms,
        {{ var('ops_caution_gust_ms') }} as limit_caution_gust_ms,
        {{ var('ops_nogo_gust_ms') }} as limit_nogo_gust_ms,

        -- Rolling context over the last 7 sols (RANGE on the sol number, so gaps are not bridged).
        avg(temp_avg_c) over w7 as temp_avg_7sol_c,
        avg(wind_avg_ms) over w7 as wind_avg_7sol_ms,
        avg(pressure_avg_pa) over w7 as pressure_avg_7sol_pa,

        -- Day-over-day pressure change, only when the previous sol really is sol - 1.
        case
            when lag(sol) over (order by sol) = sol - 1
                then pressure_avg_pa - lag(pressure_avg_pa) over (order by sol)
        end as pressure_change_pa
    from weather
    window w7 as (order by sol range between 6 preceding and current row)
)

select * from scored
