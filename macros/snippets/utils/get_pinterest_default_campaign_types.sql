{%- macro get_pinterest_default_campaign_types(campaign_name) -%}

 CASE 
    WHEN {{ campaign_name }} ~* 'prospecting' THEN 'Campaign Type: Prospecting'
    WHEN {{ campaign_name }} ~* 'retargeting' THEN 'Campaign Type: Retargeting'
    WHEN {{ campaign_name }} ~* 'react' THEN 'Campaign Type: Reactivation'
    ELSE ''
    END AS campaign_type_default

{%- endmacro -%}