{%- macro get_pinterest_clean_field(table_name, column_name) %}

    {%- if column_name == 'updated_time' -%}
    {{column_name}}

    {%- elif "_id" in column_name -%}
    NULLIF({{ column_name }}::varchar,'') as {{ column_name }}

    {#- /* Apply to specific table */ -#}
    {%- elif table_name == 'ad_group_report' -%}
        {%- if 'in_micro_dollar' in column_name -%}
            {{ column_name }}::float/1000000 as {{ column_name.split("_in_micro_dollar")[0] }}
            
        {%- elif column_name == 'date' -%}
            {{ column_name }}::date as date

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}

    {%- elif table_name == 'campaigns' -%}
        {%- if column_name == 'daily_spend_cap' -%}
            {{column_name}}/1000000 as campaign_daily_spend_cap

        {%- else -%}
        {{column_name}} as campaign_{{column_name}}

        {%- endif -%}

    {%- elif table_name == 'advertisers' -%}
        
        {{column_name}} as advertiser_{{column_name}}

    {%- elif table_name == 'ad_group_history' -%}

        {{column_name}} as ad_group_{{column_name}}


    {#- /*  End  */ -#}
    
    {# /* Apply to all tables */ #}
    {%- else -%}
    {{column_name}}

    {%- endif -%}

{% endmacro -%}