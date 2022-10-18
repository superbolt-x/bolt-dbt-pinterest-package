{%- set selected_fields = [
    "id",
    "name",
    "currency",
    "country",
    "status",
    "updated_time"
] -%}
{%- set schema_name, table_name = 'pinterest_raw', 'advertisers' -%}

WITH staging AS 
    (SELECT
    
        {% for field in selected_fields -%}
        {{ get_pinterest_clean_field(table_name, field) }},
        {% endfor -%}
        MAX(updated_time) OVER (PARTITION BY id) as last_updated_time

    FROM {{ source(schema_name, table_name) }}
    )

SELECT *
FROM staging 
WHERE updated_time = last_updated_time