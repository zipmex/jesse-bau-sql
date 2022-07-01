---- purely trade AFIN and TOK
WITH base AS (
	SELECT
	--	DATE_TRUNC('month', t.created_at) created_at 
		t.signup_hostcountry 
		, t.ap_account_id 
		, u.is_zipup_subscribed 
		, CASE WHEN product_1_symbol IN ('1INCH', 'ADA', 'BNB', 'DOGE', 'AAVE',	'AFIN',	'ALGO',	'ALPHA', 'ANKR', 'ANT',	'ATOM',	'AVAX',	'AXS',	'BAKE',	'BAL',	'BAND',	'BAT',	'BNT'
		,	'BTS',	'BTT',	'CAKE',	'CHZ',	'COTI',	'CRV',	'CTSI',	'DASH',	'DGB',	'DOT',	'EGLD',	'EOS',	'FIL',	'FTM',	'FTT', 'GALA', 'GRT','HBAR','HOT',	'ICX', 'IOST', 'JST'
		, 'KAVA', 'KNC',	'KSM',	'LRC',	'LSK',	'MANA',	'MATIC', 'IOTA', 'NANO', 'NEO',	'OGN',	'ONE',	'ONT',	'PAX',	'QTUM',	'REN',	'RLC',	'RUNE',	'RVN',	'SLP',	'SNX',	'SOL'
		,	'SRM',	'STORJ', 'SUSHI', 'TFUEL', 'THETA',	'TRX',	'UMA',	'UNI',	'VET', 'SUSHI', 'TOK', 'WAVES', 'WRX',	'WTC',	'XEM',	'XTZ',	'XVG',	'XVS',	'ZEN',	'ZIL',	'ZRX')
				THEN 'afin_tok' 
				ELSE 'other' END AS asset_group
		, SUM(t.amount_usd) "sum_usd_volume" 
	FROM 
		analytics.trades_master t
		LEFT JOIN analytics.users_master u
			ON t.ap_account_id = u.ap_account_id
	WHERE 
		t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
	GROUP BY 1,2,3,4
	ORDER BY 1,2,3,4
)	, tokafin_trader AS (
	SELECT 
		*
		, COUNT(ap_account_id) OVER(PARTITION BY ap_account_id) id_count
	FROM base
)	, zipup_balance AS (
	SELECT 
		a.created_at 
		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
		, a.ap_account_id , email
		, CASE WHEN a.ap_account_id IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 11045)
				THEN TRUE ELSE FALSE END AS is_nominee 
		, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
		, a.symbol 
		, u.zipup_subscribed_at 
		, u.is_zipup_subscribed 
		, trade_wallet_amount
		, z_wallet_amount
		, ziplock_amount
		, r.price usd_rate 
		, CASE	WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
				WHEN r.product_type = 2 THEN trade_wallet_amount * r.price 
				END AS trade_wallet_amount_usd
		, z_wallet_amount * r.price z_wallet_amount_usd
		, ziplock_amount * r.price ziplock_amount_usd
	FROM 
		analytics.wallets_balance_eod a 
		LEFT JOIN 
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol 
		    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE 
		a.created_at >= '2021-09-01 00:00:00' AND a.created_at < DATE_TRUNC('day', NOW()) 
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
		AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND a.symbol NOT IN ('TST1','TST2')
		AND a.symbol IN ('BTC','ETH','GOLD','LTC','USDT', 'USDC')
	ORDER BY 1 DESC 
	)	, aum_snapshot AS (
	SELECT 
		DATE_TRUNC('month', created_at) created_at
		, signup_hostcountry
		, ap_account_id
		, CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at)
				AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT') 
				THEN TRUE ELSE FALSE END AS is_zipup_amount
		, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
		, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
		, SUM( COALESCE (ziplock_amount, 0)) ziplock_amount
		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		, SUM(COALESCE (z_wallet_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0)) z_wallet_usd_balance
	FROM 
		zipup_balance 
	WHERE 
		is_asset_manager = FALSE AND is_nominee = FALSE
	GROUP BY 
		1,2,3,4
	ORDER BY 
		1 DESC 
)
SELECT 
	DATE_TRUNC('month', tm.created_at) created_at 
	, t.signup_hostcountry 
	, t.ap_account_id 
	, t.is_zipup_subscribed
	, tm.product_1_symbol 
	, a.z_wallet_amount_usd
	, a.ziplock_amount_usd
		, COUNT(DISTINCT tm.order_id) "count_orders"
		, COUNT(DISTINCT tm.trade_id) "count_trades"
	--	, COUNT(DISTINCT t.execution_id) "count_executions"
		, SUM(tm.quantity) "sum_coin_volume"
		, SUM(tm.amount_usd) "sum_usd_volume" 
FROM 
	tokafin_trader t 
	LEFT JOIN analytics.trades_master tm 
		ON t.ap_account_id = tm.ap_account_id 
	LEFT JOIN aum_snapshot a 
		ON t.ap_account_id = a.ap_account_id 
		AND tm.created_at = a.created_at 
WHERE 
	t.id_count = 1
	AND t.asset_group = 'afin_tok'
	AND DATE_TRUNC('day', tm.created_at) >= '2021-09-01 00:00:00' AND DATE_TRUNC('day', tm.created_at) < DATE_TRUNC('day', NOW())
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1
;