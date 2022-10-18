{%- set currency_fields = [
    "spend"
]
-%}

{%- set exclude_fields = [
    "_fivetran_synced",
    "ad_group_name",
    "ad_group_status",
    "campaign_name",
    "total_impression_frequency",
    "total_impression_user",
    "total_click_checkout",
    "total_click_checkout_quantity",
    "total_click_checkout_value_in_micro_dollar",
    "total_view_checkout",
    "total_view_checkout_quantity",
    "total_view_checkout_value_in_micro_dollar",
    "total_click_custom",
    "total_click_custom_value_in_micro_dollar",
    "total_view_custom",
    "total_view_custom_value_in_micro_dollar",
    "total_click_lead",
    "total_view_lead"
]
-%}

{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_pinterest_ad_groups_insights'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    -%}  

WITH 
    {% if var('currency') != 'USD' -%}
    currency AS
    (SELECT DISTINCT date, "{{ var('currency') }}" as raw_rate, 
        LAG(raw_rate) ignore nulls over (order by date) as exchange_rate
    FROM utilities.dates 
    LEFT JOIN utilities.currency USING(date)
    WHERE date <= current_date),
    {%- endif -%}

    {%- set exchange_rate = 1 if var('currency') == 'USD' else 'exchange_rate' %}
    
    insights AS 
    (SELECT 
        {%- for field in stg_fields -%}
        {%- if field in currency_fields or '_value' in field %}
        "{{ field }}"::float/{{ exchange_rate }} as "{{ field }}"
        {%- else %}
        "{{ field }}"
        {%- endif -%}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ ref('_stg_pinterest_ad_groups_insights') }}
    {%- if var('currency') != 'USD' %}
    LEFT JOIN currency USING(date)
    {%- endif %}
    ),

    adgroups AS 
    (SELECT ad_group_id, ad_group_name, ad_group_status
    FROM {{ ref('pinterest_ad_groups') }}
    ),

    campaigns AS 
    (SELECT campaign_id, campaign_name, campaign_status
    FROM {{ ref('pinterest_campaigns') }}
    ),

    advertisers AS 
    (SELECT advertiser_id, advertiser_name
    FROM {{ ref('pinterest_advertisers') }}
    )

SELECT *
FROM insights 
LEFT JOIN adgroups USING(ad_group_id)
LEFT JOIN campaigns USING(campaign_id)
LEFT JOIN advertisers USING(advertiser_id)