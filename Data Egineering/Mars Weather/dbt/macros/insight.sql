{#- Helpers for reading the InSight JSON. try_cast turns a missing/garbled value into NULL instead of failing the build. -#}

{% macro json_double(json_col, path) -%}
    try_cast(json_extract_string({{ json_col }}, '{{ path }}') as double)
{%- endmacro %}

{% macro json_int(json_col, path) -%}
    try_cast(json_extract_string({{ json_col }}, '{{ path }}') as integer)
{%- endmacro %}

{% macro json_utc_timestamp(json_col, path) -%}
    try_strptime(json_extract_string({{ json_col }}, '{{ path }}'), '%Y-%m-%dT%H:%M:%SZ')
{%- endmacro %}

{#- Fetch time encoded in the landed file name: .../insight_weather_YYYYMMDDTHHMMSSZ.json -#}
{% macro ingested_at_from_filename(filename_col) -%}
    try_strptime(
        regexp_extract({{ filename_col }}, 'insight_weather_([0-9]{8}T[0-9]{6}Z)[.]json$', 1),
        '%Y%m%dT%H%M%SZ'
    )
{%- endmacro %}
