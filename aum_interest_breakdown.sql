WITH base AS (
SELECT 
	a.created_at 
	, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
	, a.ap_account_id 
	, CASE WHEN a.ap_account_id IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029)
			THEN TRUE ELSE FALSE END AS is_nominee 
	, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
	, a.symbol 
	, u.zipup_subscribed_at 
	, u.is_zipup_subscribed 
	, trade_wallet_amount
	, z_wallet_amount
	, ziplock_amount
	, COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) usd_rate 
	, CASE WHEN a.created_at <= '2021-09-15 00:00:00' THEN 
				( CASE WHEN a.symbol = 'USD' THEN trade_wallet_amount * 1
				ELSE trade_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z1.price, 1/e.exchange_rate) END)
		ELSE 
				( CASE WHEN a.symbol = 'USD' THEN trade_wallet_amount * 1
				ELSE trade_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END)
		END AS trade_wallet_amount_usd
	, z_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price) z_wallet_amount_usd
	, ziplock_amount * COALESCE(c.average_high_low, g.mid_price, z.price) ziplock_amount_usd
FROM 
	analytics.wallets_balance_eod a 
	LEFT JOIN 
		analytics.users_master u 
		ON a.ap_account_id = u.ap_account_id 
	LEFT JOIN oms_data_public.cryptocurrency_prices c 
	    ON ((CONCAT(a.symbol, 'USD') = c.instrument_symbol) OR (c.instrument_symbol = 'MIOTAUSD' AND a.symbol ='IOTA') OR (c.instrument_symbol = 'USDPUSD' AND a.symbol ='PAX'))
	    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
	LEFT JOIN public.daily_closing_gold_prices g 
		ON ((DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)) 
		OR (DATE_TRUNC('day', a.created_at) = '2021-07-31 00:00:00' AND DATE_TRUNC('day', g.created_at) = '2021-07-30 00:00:00'))
		AND a.symbol = 'GOLD'
	LEFT JOIN public.daily_ap_prices z
		ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at) + '1 day'::INTERVAL
		AND ((z.instrument_symbol = 'ZMTUSD' AND a.symbol = 'ZMT')
		OR (z.instrument_symbol = 'C8PUSDT' AND a.symbol = 'C8P'))
	LEFT JOIN public.daily_ap_prices z1
		ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z1.created_at)
		AND ((z1.instrument_symbol = 'ZMTUSD' AND a.symbol = 'ZMT')
		OR (z1.instrument_symbol = 'C8PUSDT' AND a.symbol = 'C8P'))
	LEFT JOIN oms_data_public.exchange_rates e
		ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
		AND e.product_2_symbol  = a.symbol
		AND e."source" = 'coinmarketcap'
WHERE 
	a.created_at >= '2021-01-01 00:00:00' AND a.created_at < DATE_TRUNC('month', NOW()) 
	AND u.signup_hostcountry IN ('TH','ID','AU','global')
	AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
	AND a.symbol NOT IN ('TST1','TST2')
--	AND a.symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
--	AND a.ap_account_id = 
ORDER BY 1 DESC 
)
	SELECT 
		DATE_TRUNC('month', created_at) created_at 
		, signup_hostcountry
	--	, ap_account_id 
	--	, symbol 
		, CASE WHEN symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH') THEN 'zipup_coin'
				WHEN symbol IN ('ZMT') THEN 'ZMT'
				ELSE 'non_zipup' END AS asset 
		, CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at)
				AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT') THEN TRUE 
				ELSE FALSE END AS is_zipup_amount
		, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
		, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
--		, SUM( COALESCE (ziplock_amount, 0)) ziplock_amount
		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
--		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
	FROM 
		base 
	WHERE 
		is_asset_manager = FALSE AND is_nominee = FALSE 
	GROUP BY 
		1,2,3,4
	ORDER BY 
		1 DESC 
;


SELECT 
*
FROM analytics.wallets_balance_eod
ORDER BY 1 DESC 