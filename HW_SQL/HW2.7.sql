CREATE OR REPLACE FUNCTION url_d(input text) RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE 
    bin bytea := ''; -- Инициализация пустого бинарного значения
    byte text; -- Переменная для обработки символов
BEGIN 
    RAISE LOG '01--%', input; -- Логируем входное значение

    -- Цикл по всем символам строки
    FOR byte IN (
        SELECT (regexp_matches(input, '(%[0-9A-Fa-f]{2}|.)', 'g'))[1]
    ) LOOP
        -- Если символ закодирован, декодируем
        IF length(byte) = 3 THEN
            bin := bin || decode(substring(byte, 2, 2), 'hex');
        ELSE 
            -- Если символ обычный, добавляем его в результат
            bin := bin || convert_to(byte, 'UTF8');
        END IF;
    END LOOP;

    -- Преобразуем бинарное значение обратно в текст
    RETURN convert_from(bin, 'UTF8');

EXCEPTION
    -- Обработка ошибок с логированием и возвратом NULL
    WHEN OTHERS THEN 
        RAISE LOG '02--Помилка при декодуванні: %', byte;
        RETURN NULL;
END $$;

WITH 
fb_merged AS (
    SELECT
	    f_b.ad_date
	    , COALESCE (fc.campaign_name, 'Unknown Campaign') AS campaign_coalesce
	    , COALESCE (fa.adset_name, 'Unknown Adset') AS adset__coalesce
	    , COALESCE (f_b.spend, 0) AS spend_coalesce
	    , COALESCE (f_b.impressions, 0) AS impressions_coalesce
	    , COALESCE (f_b.clicks, 0) AS clicks_coalesce
	    , COALESCE (f_b.reach, 0) AS reach_coalesce
	    , COALESCE (f_b.leads, 0) AS leads_coalesce
	    , COALESCE (f_b.value, 0) AS value_coalesce
	    , CASE
	        WHEN LOWER(SUBSTRING( f_b.url_parameters FROM 'utm_campaign=([^&]+)' ) ) = 'nan' THEN NULL
			ELSE LOWER( SUBSTRING( f_b.url_parameters FROM 'utm_campaign=([^&]+)'))
	    END AS utm_campaign
	FROM facebook_ads_basic_daily AS f_b
	INNER JOIN facebook_campaign AS fc ON f_b.campaign_id = fc.campaign_id
	INNER JOIN facebook_adset AS fa ON f_b.adset_id = fa.adset_id
)
, fb_gb_merged AS (
    SELECT 
	    g_b.ad_date
	    , COALESCE (g_b.campaign_name, 'Unknown Campaign') AS campaign_name
	    , COALESCE (g_b.adset_name, 'Unknown Adset') AS adset_name
	    , COALESCE (g_b.spend, 0) AS spend_coalesce
	    , COALESCE (g_b.impressions, 0) AS impressions_coalesce
	    , COALESCE (g_b.clicks, 0) AS clicks_coalesce
	    , COALESCE (g_b.reach, 0) AS reach_coalesce
	    , COALESCE (g_b.leads, 0) AS leads_coalesce
	    , COALESCE (g_b.value, 0) AS value_coalesce
	    , CASE
	        WHEN LOWER( SUBSTRING( g_b.url_parameters  FROM 'utm_campaign=([^&]+)')) = 'nan' THEN NULL
			ELSE LOWER(SUBSTRING( g_b.url_parameters FROM 'utm_campaign=([^&]+)' ))
	    END AS utm_campaign
	FROM google_ads_basic_daily AS g_b
	UNION
	SELECT *
	FROM fb_merged
)
, first_cte AS (
    SELECT 
    ad_date
	    , date_trunc('month', ad_date)::date AS ad_month
	    ,campaign_name
	    ,adset_name
	    , SUM(spend_coalesce) AS spend_total
	    , SUM(impressions_coalesce) AS impressions_total
	    , SUM(reach_coalesce) AS reach_total
	    , SUM(clicks_coalesce) AS clicks_total
	    , SUM(leads_coalesce) AS leads_total
	    , SUM(value_coalesce) AS value_total
--	    , utm_campaign
	    , url_d(utm_campaign) AS decoded_utm_campaign
	    , CASE
	        WHEN sum(impressions_coalesce) = 0 THEN '0%'
			ELSE round(sum(clicks_coalesce) / sum(impressions_coalesce * 1.00) * 100.00, 2) || '%'
	    	END AS CTR_percent
	    , CASE
	        WHEN sum(impressions_coalesce) = 0 THEN 0
			ELSE round(sum(spend_coalesce) * 1.00 / sum(impressions_coalesce) * 1000, 2 )
	    	END AS CPM_cents
		, CASE
	        WHEN sum(clicks_coalesce) = 0 THEN 0
			ELSE round( sum(spend_coalesce) * 1.00 / sum(clicks_coalesce), 2)
			END AS CPC_cents
		, CASE
	        WHEN sum(spend_coalesce) = 0 THEN '0%'
			ELSE round(( (sum(value_coalesce) * 1.00 / sum(spend_coalesce) ) -1) * 100, 2) || '%'
			END AS ROMI_percent
	FROM fb_gb_merged
	GROUP BY 
	ad_date
		,date_trunc('month', ad_date)
		, decoded_utm_campaign
		, campaign_name
		, adset_name
)
SELECT 
	ad_month
	, to_char (ad_date, 'YYYY Month') AS month_year_str
	, decoded_utm_campaign
	, spend_total
	, impressions_total
	, clicks_total
	, value_total
	, CTR_percent
	, CPC_cents
	, CPM_cents
	, ROMI_percent
FROM first_cte
ORDER BY ad_month
LIMIT 100;

DROP FUNCTION IF EXISTS url_d;