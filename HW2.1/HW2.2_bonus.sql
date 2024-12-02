SELECT campaign_id,
    total_spend,
    total_value,
    ((total_value::numeric / total_spend::numeric) -1) as ROMI
from (
        SELECT campaign_id,
            sum(spend) as total_spend,
            sum(value) as total_value
        FROM facebook_ads_basic_daily
        GROUP BY campaign_id
    ) AS aggregated_data
WHERE total_spend > 500000
ORDER BY romi DESC
LIMIT 1;