{#-
    The API returns a nested document whose sol numbers are JSON *keys* ({"675": {...}, "676": {...}}),
    and the rolling window means every sol arrives in several files. This model:
      1. turns the dynamic keys into rows (one row per file x sol),
      2. keeps only the most recently landed copy of each sol (latest wins, so a revised
         value from NASA replaces the older one),
      3. leaves the per-sol JSON untouched for the typed models downstream.
-#}

with files as (
    select
        filename as source_file,
        {{ ingested_at_from_filename('filename') }} as ingested_at,
        content::json as content
    from {{ source('raw', 'insight_weather_files') }}
),

exploded as (
    select
        source_file,
        ingested_at,
        content,
        unnest(from_json(json_extract(content, '$.sol_keys'), '["VARCHAR"]')) as sol_key
    from files
),

payloads as (
    select
        cast(sol_key as integer) as sol,
        source_file,
        ingested_at,
        json_extract(content, '$."' || sol_key || '"') as sol_json,
        json_extract(content, '$.validity_checks."' || sol_key || '"') as validity_json,
        count(*) over (partition by sol_key) as times_ingested
    from exploded
)

select *
from payloads
qualify row_number() over (partition by sol order by ingested_at desc, source_file desc) = 1
