-- SELECT ad_date
-- , spend
-- , clicks
-- , (spend*((clicks>0)::int))/NULLIF(clicks,0) sp_cl
-- FROM facebook_ads_basic_daily
-- ORDER BY ad_date DESC
-- LIMIT 10;

SELECT ad_date
, spend
, clicks
, spend/clicks as CPC
FROM facebook_ads_basic_daily
WHERE clicks>0
ORDER BY ad_date DESC
LIMIT 20;