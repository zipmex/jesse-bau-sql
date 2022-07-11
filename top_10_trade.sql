-- top 10 asset - daily trade volume
WITH monthly_trade AS (
	SELECT 
		DATE_TRUNC('year', created_at)::DATE created_at
		, signup_hostcountry
		, product_1_symbol 
		, SUM(amount_usd) sum_trade_vol
	FROM 
		analytics.trades_master t
	WHERE 
		DATE_TRUNC('day', t.created_at) >= DATE_TRUNC('year', NOW()) 
		AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
		AND CASE WHEN t.created_at < '2022-05-05' THEN (t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping))
				ELSE (t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121)))
				END
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
	GROUP BY 1,2,3
	)	, top_10_month AS (
	SELECT 
		*
		, ROW_NUMBER () OVER(PARTITION BY created_at, signup_hostcountry ORDER BY sum_trade_vol DESC) rank_ 
	FROM monthly_trade
	)
	SELECT 
	*
	FROM top_10_month
	
	)
SELECT
	DATE_TRUNC('day', t.created_at)::DATE traded_date 
	, DATE_TRUNC('week', t.created_at)::DATE traded_week 
	, t1.product_1_symbol
	, t.signup_hostcountry 
	, COUNT(DISTINCT t.order_id) "count_orders"
	, COUNT(DISTINCT t.trade_id) "count_trades"
	, SUM(t.quantity) "sum_coin_volume"
	, SUM(t.amount_usd) "sum_usd_volume" 
FROM top_10_month t1
	LEFT JOIN
		analytics.trades_master t
		ON t1.product_1_symbol = t.product_1_symbol
WHERE 
	DATE_TRUNC('day', t.created_at) >= DATE_TRUNC('month', NOW()) - '1 month'::INTERVAL 
	AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
	AND t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
	AND t.signup_hostcountry IN ('ID') -- ('TH','ID','AU','global')
	AND t1.row_ <= 10
GROUP BY 1,2,3,4
ORDER BY 1
;

WITH monthly_trade AS (
	SELECT
		DATE_TRUNC('day', t.created_at) created_at 
	--	, t1.product_1_symbol
	--	, t.signup_hostcountry 
		, COUNT(DISTINCT t.order_id) "count_orders"
		, COUNT(DISTINCT t.trade_id) "count_trades"
		, SUM(t.quantity) "sum_coin_volume"
		, SUM(t.amount_usd) "sum_usd_volume" 
	FROM 
		analytics.trades_master t
	WHERE 
		DATE_TRUNC('day', t.created_at) >= '2021-01-01'
		AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
		AND t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
	GROUP BY 1
	ORDER BY 1	
)
SELECT
	*
	, SUM(sum_usd_volume) OVER(PARTITION BY DATE_TRUNC('month', created_at) ORDER BY created_at) cumulative_total_trade_vol
--	, SUM(sum_usd_volume) OVER(PARTITION BY DATE_TRUNC('month', created_at) , signup_hostcountry ORDER BY created_at) cumulative_country_trade_vol
FROM monthly_trade
;

