{{ config (
    alias = target.database + '_pinterest_performance_by_ad_group'
)}}

SELECT *,
    {{ get_date_parts('date') }},
    {{ get_pinterest_default_campaign_types('campaign_name')}}

FROM {{ ref('pinterest_ad_groups_insights') }}