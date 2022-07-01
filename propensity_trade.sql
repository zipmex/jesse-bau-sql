SELECT
	DATE_TRUNC('month', t.created_at) created_at
	, t.signup_hostcountry 
	, t.ap_account_id 
--	, CONCAT(t.product_1_symbol, t.product_2_symbol) instrument_symbol
	, t.product_1_symbol
--	, CASE WHEN order_type = 2 THEN 'Limit' WHEN order_type = 1 THEN 'Market' END AS order_type 
--	, t.side 
--	, CASE WHEN t.counter_party IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) THEN FALSE ELSE TRUE END "is_organic_trade" 
--	, CASE WHEN t.product_1_symbol = 'ZMT' THEN TRUE ELSE FALSE END AS is_zmt 
	, CASE WHEN w.ziplock_amount >= 20000 THEN 'vip4'
			WHEN w.ziplock_amount >= 5000 AND w.ziplock_amount < 20000 THEN 'vip3'
			WHEN w.ziplock_amount >= 1000 AND w.ziplock_amount < 5000 THEN 'vip2'
			WHEN w.ziplock_amount >= 100 AND w.ziplock_amount < 1000 THEN 'vip1'
			ELSE 'vip0'
			END AS vip_tier
	, s.survey ->> 'total_estimate_monthly_income' total_estimate_monthly_income
	, COUNT(DISTINCT t.order_id) "count_orders"
	, COUNT(DISTINCT t.trade_id) "count_trades"
	, COUNT(DISTINCT t.execution_id) "count_executions"
	, SUM(t.quantity) "sum_coin_volume"
	, SUM(t.amount_usd) "sum_usd_volume" 
FROM 
	analytics.trades_master t
	LEFT JOIN analytics.users_master u
		ON t.ap_account_id = u.ap_account_id
	LEFT JOIN analytics.wallets_balance_eod w
		ON t.ap_account_id = w.ap_account_id 
		AND w.symbol = 'ZMT'
		AND w.created_at = DATE_TRUNC('month', w.created_at)
		AND DATE_TRUNC('month', t.created_at) = DATE_TRUNC('month', w.created_at)
	LEFT JOIN user_app_public.suitability_surveys s
		ON u.user_id = s.user_id 
		AND s.archived_at IS NULL
WHERE 
	DATE_TRUNC('day', t.created_at) >= '2021-01-01 00:00:00' AND DATE_TRUNC('day', t.created_at) < '2021-10-01 00:00:00' -- DATE_TRUNC('day', NOW())
	AND t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
	AND t.signup_hostcountry IN ('TH')
--	AND t.ap_account_id = 143639
GROUP BY 1,2,3,4,5,6
ORDER BY 1
;


SELECT  
--	survey ->> 'total_estimate_monthly_income' total_estimate_monthly_income
--	survey ->> 'investment_period' investment_period
--	survey ->> 'digital_assets_experience' digital_assets_experience
--	survey ->> 'understand_digital_assets' understand_digital_assets
	survey ->> 'occupation' occupation
	s.survey ->> 'education' education
FROM user_app_public.suitability_surveys s
WHERE archived_at IS NULL 