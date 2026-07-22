{{ config (
    alias = target.database + '_pinterest_performance_by_ad_group',
    materialized = 'incremental',
    unique_key = 'unique_key',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns'
)}}

{#-
    Ad group performance, all date granularities in one table.

    Reads the day-grain incremental feeder (pinterest_performance_by_ad_group_daily),
    rolls up day/week/month/quarter/year, and joins ad group / campaign /
    advertiser metadata built inline from the raw history tables. This keeps the
    pinterest_ad_groups_insights / pinterest_ad_groups / pinterest_campaigns /
    pinterest_advertisers models out of this model's DAG so a selective run stays
    slim (mirrors the Facebook performance_by_ad_daily + performance_by_ad pattern).
-#}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set exclude_fields = ['date','last_updated','unique_key'] -%}
{%- set dimensions = ['advertiser_id','campaign_id','ad_group_id'] -%}
{%- set measures = adapter.get_columns_in_relation(ref('pinterest_performance_by_ad_group_daily'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    |reject("in",dimensions)
                    |list
                    -%}

WITH
    insights AS
    (SELECT *
    FROM {{ ref('pinterest_performance_by_ad_group_daily') }}
    {% if is_incremental() -%}
    -- Reprocess whole periods from the start of the year containing (max date - 30d)
    -- so the week/month/quarter/year roll-ups stay complete; older data is stable.
    -- Run --full-refresh periodically to refresh ad-object names on historical rows.
    WHERE date >= date_trunc('year', (select dateadd(day,-30,max(date)) from {{ ref('pinterest_performance_by_ad_group_daily') }}))::date
    {%- endif %}
    ),

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
    FROM insights
    GROUP BY {{ range(1, dimensions|length +2 +1)|list|join(',') }}),
    {%- endfor %}

    {#- ad group / campaign / advertiser metadata, built inline from the raw
        history tables (same logic as the pinterest_ad_groups / pinterest_campaigns
        / pinterest_advertisers models) -#}
    {% set ad_group_relations = dbt_utils.get_relations_by_pattern('pinterest_raw%', 'ad_group_history') -%}
    ad_group_raw AS
    ({{ dbt_utils.union_relations(relations = ad_group_relations) }}),

    {%- set ad_group_fields = ['campaign_id','id','name','status','updated_time'] %}
    ad_groups_staging AS
    (SELECT
        {% for field in ad_group_fields -%}
        {{ get_pinterest_clean_field('ad_group_history', field) }},
        {% endfor -%}
        MAX(updated_time) OVER (PARTITION BY id) as last_updated_time
    FROM ad_group_raw
    ),

    {% set campaign_relations = dbt_utils.get_relations_by_pattern('pinterest_raw%', 'campaign_history') -%}
    campaign_raw AS
    ({{ dbt_utils.union_relations(relations = campaign_relations) }}),

    {%- set campaign_fields = ['advertiser_id','id','name','status','daily_spend_cap','updated_time'] %}
    campaigns_staging AS
    (SELECT
        {% for field in campaign_fields -%}
        {{ get_pinterest_clean_field('campaigns', field) }},
        {% endfor -%}
        MAX(updated_time) OVER (PARTITION BY id) as last_updated_time
    FROM campaign_raw
    ),

    {% set advertiser_relations = dbt_utils.get_relations_by_pattern('pinterest_raw%', 'advertiser_history') -%}
    advertiser_raw AS
    ({{ dbt_utils.union_relations(relations = advertiser_relations) }}),

    {%- set advertiser_fields = ['id','name','currency','country','updated_time'] %}
    advertisers_staging AS
    (SELECT
        {% for field in advertiser_fields -%}
        {{ get_pinterest_clean_field('advertisers', field) }},
        {% endfor -%}
        MAX(updated_time) OVER (PARTITION BY id) as last_updated_time
    FROM advertiser_raw
    ),

    adgroups AS
    (SELECT ad_group_id, ad_group_name, ad_group_status
    FROM ad_groups_staging
    WHERE updated_time = last_updated_time),

    campaigns AS
    (SELECT campaign_id, campaign_name, campaign_status
    FROM campaigns_staging
    WHERE updated_time = last_updated_time),

    advertisers AS
    (SELECT advertiser_id, advertiser_name
    FROM advertisers_staging
    WHERE updated_time = last_updated_time)

SELECT *,
    {{ get_pinterest_default_campaign_types('campaign_name')}},
    {{ get_pinterest_scoring_objects() }},
    md5(
        coalesce(date_granularity,'')||'|'||
        coalesce(date::varchar,'')||'|'||
        coalesce(advertiser_id::varchar,'')||'|'||
        coalesce(campaign_id::varchar,'')||'|'||
        coalesce(ad_group_id::varchar,'')
    ) as unique_key
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
