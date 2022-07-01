1. Users who have traded >$50 AUD from 03 October at 00:00 ( GMT +11) 2021 to 13 October 2021 campaign ending at 23:59 (GMT +11).
2. Users who have bought 100 ZMT or more from 03 October at 00:00 ( GMT +11) 2021 to 13 October 2021 campaign ending at 23:59 (GMT +11)
------

WITH base AS (
	SELECT 
		DATE_TRUNC('day', t.created_at AT time ZONE 'Australia/Sydney') created_at_gmt11
		, t.ap_account_id , u.user_id 
		, t.signup_hostcountry 
		, t.product_1_symbol 
		, side
		, r.exchange_rate usd_aud
		, SUM(quantity) sum_trade_amount
		, SUM(amount_usd) sum_trade_amount_usd
	FROM 
		analytics.trades_master t
		LEFT JOIN analytics.users_master u
			ON t.ap_account_id = u.ap_account_id 
		LEFT JOIN oms_data_public.exchange_rates r 
			ON DATE_TRUNC('day', t.created_at) = DATE_TRUNC('day', r.created_at)
			AND r.product_2_symbol = 'AUD'
	WHERE
		t.created_at AT time ZONE 'Australia/Sydney' >= '2021-10-03 00:00:00'
		AND t.created_at AT time ZONE 'Australia/Sydney' < '2021-10-14 00:00:00'
		AND t.ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443'
			,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','44057','161347','316078','44056','63152')
		AND t.signup_hostcountry IN ('AU')
	GROUP BY 1,2,3,4,5,6,7
	ORDER BY 1,2
)	, aud_conversion AS (
	SELECT
		*
		, sum_trade_amount_usd * usd_aud sum_trade_amount_aud
	FROM base
)	, zmt_lock AS (
	SELECT
		DATE_TRUNC('month', l.created_at AT time ZONE 'Australia/Sydney') created_at
		, u.ap_account_id 
		, l.service_id 
		, l.product_id 
		, SUM(l.credit) new_zmt_lock_amount
	FROM asset_manager_public.ledgers l 
		LEFT JOIN analytics.users_master u
		ON l.account_id = u.user_id 
	WHERE 
		l.created_at AT time ZONE 'Australia/Sydney' >= '2021-10-03 00:00:00'
		AND l.created_at AT time ZONE 'Australia/Sydney' < '2021-10-14 00:00:00'
		AND l.product_id LIKE 'zmt%'
		AND l.service_id = 'zip_lock'
		AND u.signup_hostcountry = 'AU'
	GROUP BY 1,2,3,4
)	, trader_50aud AS (
	SELECT
		DATE_TRUNC('month', a.created_at_gmt11) created_at_gmt11
		, a.signup_hostcountry 
		, a.ap_account_id , a.user_id
		, COALESCE (new_zmt_lock_amount, 0) new_zmt_lock_amount
		, SUM(sum_trade_amount) sum_trade_amount
		, SUM(sum_trade_amount_usd) sum_trade_amount_usd
		, SUM(sum_trade_amount_aud) sum_trade_amount_aud
	FROM aud_conversion a
		LEFT JOIN zmt_lock z 
		ON a.ap_account_id = z.ap_account_id
	GROUP BY 1,2,3,4,5
)	, trader_100zmt AS (
	SELECT
		DATE_TRUNC('month', a.created_at_gmt11) created_at_gmt11
		, a.signup_hostcountry 
		, a.ap_account_id , a.user_id
		, COALESCE (new_zmt_lock_amount, 0) new_zmt_lock_amount
		, SUM(sum_trade_amount) sum_trade_amount
		, SUM(sum_trade_amount_usd) sum_trade_amount_usd
		, SUM(sum_trade_amount_aud) sum_trade_amount_aud
	FROM aud_conversion a 
		LEFT JOIN zmt_lock z 
		ON a.ap_account_id = z.ap_account_id
	WHERE 
		side = 'Buy'
		AND product_1_symbol = 'ZMT'
	GROUP BY 1,2,3,4,5
)
SELECT * 
, CASE WHEN sum_trade_amount_aud >= 50 THEN '50aud' ELSE 'less50aud' END AS is_valid FROM trader_50aud
UNION ALL
SELECT *
, CASE WHEN sum_trade_amount >= 100 AND new_zmt_lock_amount >= 100 THEN '100zmt' ELSE 'less100zmt' END AS is_valid FROM trader_100zmt
;



SELECT 
	NOW()
	, NOW() AT time ZONE 'Australia/Sydney'