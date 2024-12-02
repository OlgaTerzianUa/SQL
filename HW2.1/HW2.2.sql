SELECT ad_date,
    campaign_id,
    total_spend,
    total_impressions,
    total_clicks,
    total_value,
    total_spend::numeric / total_clicks::numeric as CPC,
    (
        total_spend::numeric / total_impressions::numeric
    ) * 1000 as CPM,
    (
        total_clicks::numeric / total_impressions::numeric
    ) * 100 as CTR,
    ((total_value::numeric / total_spend::numeric) -1) as ROMI
from (
        SELECT ad_date,
            campaign_id,
            sum(spend) as total_spend,
            sum(impressions) as total_impressions,
            sum(clicks) as total_clicks,
            sum(value) as total_value
        FROM facebook_ads_basic_daily
        GROUP BY ad_date,
            campaign_id
    ) AS aggregated_data
WHERE total_spend > 0
    and total_impressions > 0
    and total_clicks > 0
    and total_value > 0;