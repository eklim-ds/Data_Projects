{#- One row per landed API response: what each batch run actually delivered. Feeds the pipeline-health audit. -#}

select
    filename as source_file,
    {{ ingested_at_from_filename('filename') }} as ingested_at,
    size as size_bytes,
    list_transform(
        from_json(json_extract(content, '$.sol_keys'), '["VARCHAR"]'),
        sol_key -> cast(sol_key as integer)
    ) as sol_keys
from {{ source('raw', 'insight_weather_files') }}
