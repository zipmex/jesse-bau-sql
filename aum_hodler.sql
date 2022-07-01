-- AUM hodler, no trade within the month
WITH active_trader AS (
-- find active trade monthly to exclude in AUM
	SELECT 
		DISTINCT tm.ap_account_id
		, DATE_TRUNC('month', tm.created_at) trade_month
	FROM analytics.trades_master tm
	WHERE 
		tm.created_at >= '2021-01-01'              
		AND tm.created_at < '2022-01-01'                        
)	, aum_monthly AS (
	SELECT 
		a.created_at 
		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
		, a.ap_account_id 
		, CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id <> 496001)
				THEN TRUE ELSE FALSE END AS is_nominee 
		, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
		, a.symbol 
		, u.zipup_subscribed_at 
		, u.is_zipup_subscribed 
		, trade_wallet_amount
		, z_wallet_amount
		, ziplock_amount
		, COALESCE (r.price , rm.price) usd_rate 
		, CASE	WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
				WHEN COALESCE (r.product_type, rm.product_type) = 2 THEN trade_wallet_amount * COALESCE (r.price , rm.price)
				END AS trade_wallet_amount_usd
		, z_wallet_amount * r.price z_wallet_amount_usd
		, ziplock_amount * r.price ziplock_amount_usd
	FROM 
		analytics.wallets_balance_eod a 
		LEFT JOIN active_trader tm 
			ON a.ap_account_id = tm.ap_account_id
			AND DATE_TRUNC('month', a.created_at) = tm.trade_month
		LEFT JOIN 
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
		LEFT JOIN 
			analytics.rates_master rm
			ON a.symbol = 'GOLD' 
			AND a.created_at::date > '2021-07-30' 
			AND a.created_at::date < '2021-08-02'
			AND (rm.product_1_symbol = 'GOLD' AND rm.created_at::date = '2021-07-30')
	WHERE 
		a.created_at >= '2021-01-01' AND a.created_at < DATE_TRUNC('month', NOW()) 
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
		AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND a.symbol NOT IN ('TST1','TST2')
	-- excclude active trader everymonth
		AND tm.ap_account_id IS NULL
	ORDER BY 1 DESC 
)--	, aum_snapshot AS (
	SELECT 
		DATE_TRUNC('day', created_at) created_at
		, signup_hostcountry
		, ap_account_id
--		, symbol
		, CASE WHEN symbol IN ('BTC','ETH','GOLD','LTC','USDC','USDT') THEN 'zipup_coin' 
				WHEN symbol = 'ZMT' THEN 'ZMT' 
				ELSE 'other' END AS asset_group
		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		, SUM( COALESCE (CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
					THEN
						(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
								WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
					END, 0)) AS zwallet_subscribed_usd
		, SUM( COALESCE (trade_wallet_amount, 0) + COALESCE (z_wallet_amount, 0) + COALESCE (ziplock_amount, 0)) total_coin_amount
		, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0)) total_aum_usd
	FROM 
		aum_monthly 
	WHERE 
		is_asset_manager = FALSE AND is_nominee = FALSE
	GROUP BY 
		1,2,3,4
	ORDER BY 
		1 
;