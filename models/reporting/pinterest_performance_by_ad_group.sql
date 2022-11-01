{{ config (
    alias = target.database + '_pinterest_performance_by_ad_group'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set exclude_fields = ['date','last_updated','unique_key'] -%}
{%- set dimensions = ['advertiser_id','campaign_id','ad_group_id'] -%}
{%- set measures = adapter.get_columns_in_relation(ref('pinterest_ad_groups_insights'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    |reject("in",dimensions)
                    |list
                    -%}  

WITH 
    {%- for date_granularity in date_granularity_list %}

    performance_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        DATE_TRUNC('{{date_granularity}}', date) as date,
        {%- for dimension in dimensions %}
        {{ dimension }},
        {%-  endfor %}
        {% for measure in measures -%}
        COALESCE(SUM({{ measure }}),0) as {{ measure }}
        {%- if not loop.last %},{%- endif %}
        {% endfor %}
    FROM {{ ref('pinterest_ad_groups_insights') }}
    GROUP BY {{ range(1, dimensions|length +2 +1)|list|join(',') }}),
    {%- endfor %}

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


SELECT *,
    {{ get_pinterest_default_campaign_types('campaign_name')}},
    {{ get_pinterest_scoring_objects() }}
FROM 
    ({% for date_granularity in date_granularity_list -%}
    SELECT *
    FROM performance_{{date_granularity}}
    {% if not loop.last %}UNION ALL
    {% endif %}

    {%- endfor %}
    )
LEFT JOIN adgroups USING(ad_group_id)
LEFT JOIN campaigns USING(campaign_id)
LEFT JOIN advertisers USING(advertiser_id)