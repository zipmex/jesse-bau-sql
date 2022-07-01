---- AUM btc dominance
WITH base AS (
	SELECT 
		a.created_at 
		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
		, a.ap_account_id , email
		, CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id <> 496001)
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
		a.created_at >= '2021-11-01 00:00:00' AND a.created_at < DATE_TRUNC('day', NOW()) 
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
		AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND a.symbol NOT IN ('TST1','TST2')
	ORDER BY 1 DESC 
)	, aum_snapshot AS (
	SELECT 
		created_at 
	--	, signup_hostcountry
	--	, ap_account_id , email
		, CASE 	WHEN symbol IN ('BTC', 'ETH') THEN symbol --'zipup_coin'
				WHEN symbol IN ('USDT', 'USDC') THEN 'usdc_usdt'
				WHEN symbol IN ('ZMT') THEN 'ZMT'
				WHEN symbol IN ('USD','SGD','THB','IDR','AUD','VND') THEN 'cash'
				ELSE 'other' END AS asset_group
		, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
		, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
		, SUM( COALESCE (ziplock_amount, 0)) ziplock_amount
		, SUM( COALESCE (trade_wallet_amount, 0) + COALESCE (z_wallet_amount, 0) + COALESCE (ziplock_amount, 0)) total_coin_amount
		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0)) total_usd_amount
	FROM 
		base 
	WHERE 
		is_asset_manager = FALSE AND is_nominee = FALSE
	GROUP BY 
		1,2
	ORDER BY 
		1 DESC
)	, monthly_snapshot AS (
	SELECT
		created_at 
		, asset_group
		, total_coin_amount
		, total_usd_amount
		, SUM(total_coin_amount) OVER(PARTITION BY created_at) eom_coin_amount
		, SUM(total_usd_amount) OVER(PARTITION BY created_at) eom_usd_amount
	FROM aum_snapshot
)
SELECT 
	created_at 
	, asset_group
	, total_coin_amount/ eom_coin_amount::float coin_amount_distribution
	, total_usd_amount/ eom_usd_amount::float usd_amount_distribution
FROM monthly_snapshot 
;



---- Trade Vol BTC Dominance
WITH trade_base AS (
	SELECT
		DATE_TRUNC('month', t.created_at) created_at 
	--	, t.signup_hostcountry 
	--	, t.ap_account_id 
	--	, t.product_1_symbol
		, CASE 	WHEN product_1_symbol IN ('BTC','ETH') THEN product_1_symbol 
				WHEN product_1_symbol IN ('BNB','SOL','ADA','DOT','ATOM','LUNA','AVAX','ALGO','TRON','FTM','HBAR','ONE') THEN 'layer1' 
				WHEN product_1_symbol IN ('USDT', 'USDC') THEN 'usdc_usdt' 
				WHEN product_1_symbol IN ('AFIN','TOK') THEN 'afin_tok' 
				WHEN product_1_symbol IN ('SAND','MANA') THEN 'mana_sand' 
				ELSE 'other' END AS asset_group
		, SUM(t.quantity) "sum_coin_volume"
		, SUM(t.amount_usd) "sum_usd_volume" 
	FROM 
		analytics.trades_master t
		LEFT JOIN analytics.users_master u
			ON t.ap_account_id = u.ap_account_id
	WHERE 
		t.created_at >= '2021-01-01 00:00:00' AND t.created_at < DATE_TRUNC('day', NOW())
		AND t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
	GROUP BY 1,2
	ORDER BY 1
)	, total_trade_monthly AS (
	SELECT
		*
		, SUM(sum_coin_volume) OVER(PARTITION BY created_at) eom_coin_amount
		, SUM(sum_usd_volume) OVER(PARTITION BY created_at) eom_usd_amount
	FROM trade_base
)
SELECT
	*
	, sum_usd_volume/ eom_usd_amount::float usd_amount_distribution
FROM total_trade_monthly
;

