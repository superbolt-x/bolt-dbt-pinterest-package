{%- macro get_pinterest_scoring_objects() -%}

    TRIM(SPLIT_PART(ad_group_name, '-', 2)) as audience

{%- endmacro -%}