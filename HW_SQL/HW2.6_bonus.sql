CREATE OR REPLACE FUNCTION url_d_NON(input text) RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE bin bytea := '';
-- Ініціалізація порожнього бінарного значення
byte text;
BEGIN raise log '01--%',
input;
-- Проходимо по всіх символах рядка
FOR byte IN (
    select (
            regexp_matches(input, '(%[0-9A-Fa-f]{2}|.)', 'g')
        ) [1]
) LOOP IF length(byte) = 3 THEN -- Якщо це закодований символ (наприклад, %20), то декодуємо
bin := bin || decode(substring(byte, 2, 2), 'hex');
ELSE -- Інакше просто додаємо символ як є
bin := bin || convert_to(byte, 'UTF8');
END IF;
END LOOP;
-- Перетворюємо бінарні дані в рядок у кодуванні UTF-8
RETURN convert_from(bin, 'UTF8');
EXCEPTION
WHEN OTHERS THEN raise log '02--Помилка при декодуванні: %',
byte;
RETURN NULL;
END $$;
with fb_merged as (
    select f_b.ad_date,
        coalesce (fc.campaign_name, 'Unknown Campaign') as campaign_coalesce,
        coalesce (fa.adset_name, 'Unknown Adset') as adset__coalesce,
        coalesce (f_b.spend, 0) as spend_coalesce,
        coalesce (f_b.impressions, 0) as impressions_coalesce,
        coalesce (f_b.clicks, 0) as clicks_coalesce,
        coalesce (f_b.reach, 0) as reach_coalesce,
        coalesce (f_b.leads, 0) as leads_coalesce,
        coalesce (f_b.value, 0) as value_coalesce,
        case
            when LOWER(
                SUBSTRING(
                    f_b.url_parameters
                    from --  в PostgreSQL и некоторых других СУБД использование FROM внутри функции SUBSTRING является обязательным, когда вы работаете с регулярными выражениями. Это не просто стандартная позиция, а синтаксическая особенность для указания, что дальше следует регулярное выражение для извлечения подстроки.
                        'utm_campaign=([^&]+)' --[^&] - все кроме &, + -один или более символов 
                )
            ) = 'nan' then null
            else LOWER(
                SUBSTRING(
                    f_b.url_parameters
                    from 'utm_campaign=([^&]+)'
                )
            )
        end as utm_campaign
    from facebook_ads_basic_daily f_b
        inner join facebook_campaign fc on f_b.campaign_id = fc.campaign_id
        inner join facebook_adset fa on f_b.adset_id = fa.adset_id
),
fb_gb_merged as (
    select g_b.ad_date,
        coalesce (g_b.campaign_name, 'Unknown Campaign') as campaign_name,
        coalesce (g_b.adset_name, 'Unknown Adset') as adset_name,
        coalesce (g_b.spend, 0) as spend_coalesce,
        coalesce (g_b.impressions, 0) as impressions_coalesce,
        coalesce (g_b.clicks, 0) as clicks_coalesce,
        coalesce (g_b.reach, 0) as reach_coalesce,
        coalesce (g_b.leads, 0) as leads_coalesce,
        coalesce (g_b.value, 0) as value_coalesce,
        -- g_b.url_parameters,
        case
            when LOWER(
                SUBSTRING(
                    g_b.url_parameters
                    from 'utm_campaign=([^&]+)'
                )
            ) = 'nan' then null
            else LOWER(
                SUBSTRING(
                    g_b.url_parameters
                    from 'utm_campaign=([^&]+)'
                )
            )
        end as utm_campaign
    from google_ads_basic_daily g_b
    union
    select *
    from fb_merged
)
select ad_date,
    (campaign_name) as campaign_name,
    (adset_name) as adset_name,
    SUM(spend_coalesce) as spend_total,
    SUM(impressions_coalesce) as impressions_total,
    SUM(reach_coalesce) as reach_total,
    SUM(clicks_coalesce) as clicks_total,
    SUM(leads_coalesce) as leads_total,
    SUM(value_coalesce) as value_total,
    utm_campaign,
    url_d_NON(utm_campaign) as decoded_utm_campaign,
    case
        when sum(impressions_coalesce) = 0 then '0%'
        else round(
            sum(clicks_coalesce) / sum(impressions_coalesce * 1.00) * 100.00,
            2
        ) || '%'
    end as CTR_percent,
    case
        when sum(impressions_coalesce) = 0 then 0
        else round(
            sum(spend_coalesce) * 1.00 / sum(impressions_coalesce) * 1000,
            2
        )
    end as CPM_cents,
    case
        when sum(clicks_coalesce) = 0 then 0
        else round(
            sum(spend_coalesce) * 1.00 / sum(clicks_coalesce),
            2
        )
    end as CPC_cents,
    case
        when sum(spend_coalesce) = 0 then '0%'
        else round(
            (
                (
                    sum(value_coalesce) * 1.00 / sum(spend_coalesce)
                ) -1
            ) * 100,
            2
        ) || '%'
    end as ROMI_percent
from fb_gb_merged
group by ad_date,
    campaign_name,
    adset_name,
    utm_campaign,
    decoded_utm_campaign;
DROP FUNCTION IF EXISTS url_d_NON;