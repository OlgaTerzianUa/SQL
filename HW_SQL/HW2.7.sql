/*
Оптимизация:

Оптимизация функции url_d:
Используйте встроенные функции PostgreSQL, например, decode и regexp_matches, без использования циклов.
Перепишите url_d с использованием только SQL или PL/pgSQL без FOR, если это возможно.

Исключите ненужные данные:
Убедитесь, что выборка (SELECT) работает только с необходимыми столбцами.
Уберите ненужные записи или обработайте их заранее, используя фильтры WHERE.

Оптимизация CTE:
Если CTE используется несколько раз, замените его на временные таблицы.
Убедитесь, что PostgreSQL не вычисляет CTE заново каждый раз. Например, используйте материализованные CTE.

Добавьте индексы:
Добавьте индексы на столбцы, которые используются в соединениях (ON f_b.campaign_id = fc.campaign_id), фильтрах и группировке.
Подумайте о создании составных индексов для часто используемых комбинаций столбцов.

Объедините UNION с минимизацией вычислений:
Убедитесь, что лишние записи исключены до UNION.
Если возможно, используйте UNION ALL вместо UNION (если дубликаты не имеют значения).

Переместите вызовы функции url_d в отдельный этап:
Декодируйте URL-адреса один раз и сохраните результаты в отдельной колонке в таблице.

Используйте агрегаты эффективнее:
Минимизируйте количество операций в SUM() или других агрегатных функциях, предварительно обработав данные.
*/
CREATE OR REPLACE FUNCTION url_d(input text) RETURNS text LANGUAGE SQL IMMUTABLE STRICT AS $$
  SELECT string_agg(
    CASE
      -- Если элемент закодирован, декодируем
      WHEN elem LIKE '%__%' THEN convert_from(decode(substring(elem, 2, 2), 'hex'), 'UTF8')
      -- Иначе оставляем как есть
      ELSE elem
    END, '' -- Объединяем все элементы в одну строку
  )
  FROM (
    -- Разбиваем строку на элементы с помощью регулярного выражения и разворачиваем массив
    SELECT unnest(regexp_matches(input, '(%[0-9A-Fa-f]{2}|.)', 'g')) AS elem
  ) subquery;
$$;
/*
1 Логика обработки
2 Обработка массива
3 Производительность
4 Исключения
5 Логирование
6 Простота поддержки

Исходный (PL/pgSQL)
1 Использует цикл FOR для обработки каждого символа строки по отдельности.
2 Прямое обращение к массиву через индекс (regexp_matches(...))[1].
3 Менее производительный, так как цикл выполняется построчно, создавая накладные расходы на контекст PL/pgSQL.
4 Включает блок EXCEPTION для обработки ошибок (например, некорректной строки).
5 Логирует начало работы и возможные ошибки декодирования.
6 Более сложный для чтения из-за процедурной структуры.

Оптимизированный (SQL)
1 Использует агрегатную функцию string_agg, работающую сразу со всеми элементами.
2 Использует unnest для разворачивания массива в строки.
3 Более производительный, так как операции выполняются в одном запросе SQL.
4 Исключения не обрабатываются — предполагается корректность входных данных.
5 Логирование отсутствует.
6 Лаконичный и декларативный подход делает код проще.

Почему изменения были сделаны?
Оптимизация производительности:

В PL/pgSQL каждая итерация цикла создает дополнительный контекст выполнения, что снижает скорость.
SQL-вариант использует единовременную обработку данных через агрегатные функции, что значительно быстрее.
Упрощение кода:

SQL-вариант короче и проще, так как избегает процедурных конструкций.
Использование string_agg позволяет объединить декодированные части строки за одну операцию.
Снижение накладных расходов:

PL/pgSQL имеет более высокий overhead (контекст выполнения, переменные, обработка исключений).
SQL-вариант выполняется на уровне запроса, что требует меньше ресурсов.
Типовые ошибки:

В PL/pgSQL возможны ошибки при работе с декодированием (convert_from, decode) в контексте исключений.
SQL-вариант избегает явного управления исключениями, что уменьшает вероятность ошибок.
*/

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