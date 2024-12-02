WITH f_cte AS (
    SELECT fb.ad_date,
        fc.campaign_name,
        fa.adset_name,
        fb.spend,
        fb.impressions,
        fb.reach,
        fb.clicks,
        fb.leads,
        fb.value,
        fb.url_parameters,
        'fb' as media_sourse
    FROM facebook_ads_basic_daily fb
        LEFT JOIN facebook_adset fa ON fb.adset_id = fa.adset_id
        LEFT JOIN facebook_campaign fc ON fb.campaign_id = fc.campaign_id
),
g_cte AS (
    SELECT *,
        'google' as media_sourse
    FROM google_ads_basic_daily g
)
SELECT ad_date,
    media_sourse,
    campaign_name,
    adset_name,
    reach,
    leads,
    url_parameters,
    sum(spend) AS total_spend,
    sum(impressions) AS total_impressions,
    sum(clicks) AS total_clicks,
    sum(value) AS total_value
FROM (
        SELECT *
        from f_cte
        UNION ALL
        SELECT *
        from g_cte
    ) op
GROUP BY ad_date,
    media_sourse,
    campaign_name,
    adset_name,
    reach,
    leads,
    url_parameters
ORDER BY ad_date;