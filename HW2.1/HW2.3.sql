with cte as(
    SELECT ad_date,
        spend,
        impressions,
        clicks,
        value,
        'Goggle Ads' as media_sourse
    from google_ads_basic_daily
    UNION ALL
    SELECT ad_date,
        spend,
        impressions,
        clicks,
        value,
        'Fb Ads' as media_sourse
    from facebook_ads_basic_daily
)
SELECT ad_date,
    media_sourse,
    sum(spend) as total_spend,
    sum(impressions) as total_impressions,
    sum(clicks) as total_clicks,
    sum(value) as total_value
from cte
group by ad_date,
    media_sourse
ORDER BY ad_date,
    media_sourse;