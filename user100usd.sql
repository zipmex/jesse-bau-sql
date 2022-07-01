WITH base AS (
SELECT 
	a.created_at 
	, u.signup_hostcountry 
	, a.ap_account_id 
	, u.email 
	, u.is_zipup_subscribed 
	, a.symbol 
	, SUM(trade_wallet_amount) trade_wallet_amount
	, SUM(z_wallet_amount) z_wallet_amount
	, SUM(ziplock_amount) ziplock_amount
	, SUM( CASE WHEN a.symbol = 'USD' THEN trade_wallet_amount * 1 
			WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
			WHEN r.product_type = 2 THEN trade_wallet_amount * r.price 
			END ) AS trade_wallet_amount_usd
	, SUM( z_wallet_amount * r.price ) z_wallet_amount_usd
	, SUM( ziplock_amount * r.price ) ziplock_amount_usd
	, COUNT(DISTINCT a.ap_account_id) user_count
FROM 
	analytics.wallets_balance_eod a 
	LEFT JOIN 
		analytics.users_master u 
		ON a.ap_account_id = u.ap_account_id 
	LEFT JOIN 
		data_team_staging.rates_master_staging r 
		ON ((CONCAT(a.symbol, 'USD') = r.instrument_symbol) OR (a.symbol = 'C8P' AND r.instrument_symbol = 'C8PUSDT') OR (a.symbol = r.product_2_symbol AND r.product_type = 1))
	    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
WHERE 
	a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL  
--	AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
	AND u.signup_hostcountry IN ('TH','ID','AU','global')
	AND a.ap_account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001) 
	AND a.symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
GROUP BY 
	1,2,3,4,5,6
ORDER BY 1,2,3,4
)	, total_trade_wallet AS (
SELECT
	* 
	, SUM(trade_wallet_amount_usd) OVER(PARTITION BY ap_account_id) total_trade_wallet_usd
	, ROW_NUMBER() OVER(PARTITION BY email ORDER BY trade_wallet_amount_usd DESC) user_duplicate
FROM base
)
SELECT 
	*
	, CASE WHEN total_trade_wallet_usd >= 100 THEN TRUE ELSE FALSE END AS is_100usd
FROM total_trade_wallet