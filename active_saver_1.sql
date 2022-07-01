---- aum after nov 18 - using rates_master 
WITH base AS (
	SELECT 
		a.created_at 
		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
		, a.ap_account_id 
	-- filter nominee accounts from users_mapping
		, CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id <> 496001)
				THEN TRUE ELSE FALSE END AS is_nominee 
	-- filter asset_manager account
		, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
	-- zipup subscribe status to identify zipup amount
		, u.zipup_subscribed_at , u.is_zipup_subscribed 
		, a.symbol 
		, r.price usd_rate 
		, trade_wallet_amount
		, z_wallet_amount
		, ziplock_amount
		, zlaunch_amount
		, CASE	WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
				WHEN r.product_type = 2 THEN trade_wallet_amount * r.price
				END AS trade_wallet_amount_usd
		, z_wallet_amount * r.price z_wallet_amount_usd
		, ziplock_amount * r.price ziplock_amount_usd
		, zlaunch_amount * r.price zlaunch_amount_usd
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
		a.created_at >= '2022-04-25'
	-- exclude test products
		AND a.symbol NOT IN ('TST1','TST2')
		AND a.symbol IN ('BTC','ETH','GOLD','LTC','USDC','USDT','ZMT') 
		AND u.signup_hostcountry IN ('TH') --('AU','ID','global','TH')
	ORDER BY 1 DESC 
)	, aum_snapshot AS (
	SELECT 
		DATE_TRUNC('day', created_at)::DATE created_at
        , signup_hostcountry
		, ap_account_id 
		, CASE WHEN symbol = 'ZMT' THEN 'zmt' 
				ELSE 'non_zmt' END AS asset_group
		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		, SUM( COALESCE (CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
					THEN
						(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
								WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
					END, 0)) AS zwallet_subscribed_usd
	FROM 
		base 
	WHERE 
		is_asset_manager = FALSE AND is_nominee = FALSE
	GROUP BY 
		1,2,3,4
	ORDER BY 
		1 
)	, asset_group AS (
	SELECT 
		created_at
		, signup_hostcountry
		, ap_account_id 
		, SUM( CASE WHEN asset_group = 'zmt' THEN COALESCE (trade_wallet_amount_usd, 0) END) zmt_tw
		, SUM( CASE WHEN asset_group = 'non_zmt' THEN COALESCE (trade_wallet_amount_usd, 0) END) non_zmt_tw
		, SUM( CASE WHEN asset_group = 'zmt' THEN COALESCE (zwallet_subscribed_usd, 0) END) zmt_zw_zipup
		, SUM( CASE WHEN asset_group = 'non_zmt' THEN COALESCE (zwallet_subscribed_usd, 0) END) non_zmt_zw_zipup
		, SUM( CASE WHEN asset_group = 'zmt' THEN COALESCE (ziplock_amount_usd, 0) END) zmt_ziplock
		, SUM( CASE WHEN asset_group = 'non_zmt' THEN COALESCE (ziplock_amount_usd, 0) END) non_zmt_ziplock
	FROM 
		aum_snapshot
	GROUP BY 1,2,3
)	, zmt_nonzmt AS (
	SELECT 
		created_at
		, signup_hostcountry
		, ap_account_id 
		, CASE WHEN zmt_tw >= 1 AND non_zmt_tw >= 1 THEN 'trade_mix'
				WHEN zmt_tw >= 1 AND non_zmt_tw < 1 THEN 'trade_zmt' 
				WHEN zmt_tw < 1 AND non_zmt_tw >= 1 THEN 'trade_non_zmt' 
				END AS trade_w_group
		, CASE WHEN zmt_zw_zipup >= 1 AND non_zmt_zw_zipup >= 1 THEN 'zw_mix'
				WHEN zmt_zw_zipup >= 1 AND non_zmt_zw_zipup < 1 THEN 'zw_zmt' 
				WHEN zmt_zw_zipup < 1 AND non_zmt_zw_zipup >= 1 THEN 'zw_non_zmt' 
				END AS zw_group
		, CASE WHEN zmt_ziplock >= 1 AND non_zmt_ziplock >= 1 THEN 'ziplock_mix'
				WHEN zmt_ziplock >= 1 AND non_zmt_ziplock < 1 THEN 'ziplock_zmt' 
				WHEN zmt_ziplock < 1 AND non_zmt_ziplock >= 1 THEN 'ziplock_non_zmt' 
				END AS ziplock_group
	FROM asset_group
)
SELECT 
	created_at
	, signup_hostcountry
--	, trade_w_group
--	, zw_group
--	, ziplock_group
	, COUNT(DISTINCT CASE WHEN trade_w_group = 'trade_mix' THEN ap_account_id END) trade_mix_count
	, COUNT(DISTINCT CASE WHEN trade_w_group = 'trade_zmt' THEN ap_account_id END) trade_zmt_count
	, COUNT(DISTINCT CASE WHEN trade_w_group = 'trade_non_zmt' THEN ap_account_id END) trade_non_zmt_count
	, COUNT(DISTINCT CASE WHEN zw_group = 'zw_mix' THEN ap_account_id END) zw_mix_count
	, COUNT(DISTINCT CASE WHEN zw_group = 'zw_zmt' THEN ap_account_id END) zw_zmt_count
	, COUNT(DISTINCT CASE WHEN zw_group = 'zw_non_zmt' THEN ap_account_id END) zw_non_zmt_count
	, COUNT(DISTINCT CASE WHEN ziplock_group = 'ziplock_mix' THEN ap_account_id END) ziplock_mix_count
	, COUNT(DISTINCT CASE WHEN ziplock_group = 'ziplock_zmt' THEN ap_account_id END) ziplock_zmt_count
	, COUNT(DISTINCT CASE WHEN ziplock_group = 'ziplock_non_zmt' THEN ap_account_id END) ziplock_non_zmt_count
FROM zmt_nonzmt
GROUP BY 1,2
;























