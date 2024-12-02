-- Шаг 1: Объединить данные
WITH f_g_cte AS (
    SELECT fb.ad_date,
        fc.campaign_name,
        fa.adset_name,
        fb.spend,
        fb.impressions,
        fb.clicks,
        fb.value,
        'fb' as media_source
    FROM facebook_ads_basic_daily fb
        INNER JOIN facebook_adset fa ON fb.adset_id = fa.adset_id
        INNER JOIN facebook_campaign fc ON fb.campaign_id = fc.campaign_id
    UNION ALL
    SELECT ad_date,
        campaign_name,
        adset_name,
        spend,
        impressions,
        clicks,
        value,
        'google' as media_source
    FROM google_ads_basic_daily
),
-- Шаг 2: Найти кампании с расходами > 500 000
campaign_totals as(
    SELECT campaign_name,
        sum(spend::numeric) as total_spend,
        sum(value::numeric) as total_value,
        (
            sum(VALUE::numeric) / nullif(SUM(spend::numeric), 0) -1
        ) as romi
    FROM f_g_cte
    GROUP BY campaign_name
    HAVING sum(spend::numeric) > 500000
),
-- Шаг 3: Найти кампанию с самым высоким ROMI
top_campaign as (
    SELECT campaign_name,
        ROMI as max_romi
    from campaign_totals
    ORDER BY ROMI DESC
    LIMIT 1
) -- *Шаг 4: Найти группу объявлений (adset) с самым высоким ROMI в этой кампании
SELECT md.campaign_name,
    md.adset_name,
    sum(md.spend::numeric) as adset_spend,
    sum(md.value::numeric) as adset_value,
    (
        sum(md.VALUE::numeric) / nullif(SUM(md.spend::numeric), 0) -1
    ) as adset_romi
from f_g_cte md
    INNER JOIN top_campaign tc On md.campaign_name = tc.campaign_name
GROUP by md.campaign_name,
    md.adset_name
ORDER by adset_romi DESC
LIMIT 1;