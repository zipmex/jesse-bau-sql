---- aum after nov 18 - using rates_master 
WITH base AS (
	SELECT 
		a.created_at 
		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
		, a.ap_account_id , u.user_id 
		, CASE WHEN a.created_at < '2021-11-01 00:00:00' THEN 
				(CASE WHEN a.ap_account_id IN (0, 3, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 11045, 496001)
				THEN TRUE ELSE FALSE END)			
			ELSE
				(CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping )
				THEN TRUE ELSE FALSE END) 
			END AS is_nominee 
		, a.symbol 
		, CASE WHEN zt.zip_tier IS NULL THEN 0 ELSE 1 END AS is_pcs
		, u.zipup_subscribed_at 
		, u.is_zipup_subscribed 
		, trade_wallet_amount
		, z_wallet_amount
		, a.ziplock_amount
		, r.price usd_rate 
		, CASE	WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
				WHEN r.product_type = 2 THEN trade_wallet_amount * r.price
				END AS trade_wallet_amount_usd
		, z_wallet_amount * r.price z_wallet_amount_usd
		, a.ziplock_amount * r.price ziplock_amount_usd
	FROM 
		analytics.wallets_balance_eod a 
		LEFT JOIN 
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
		LEFT JOIN 
			analytics.zmt_tier_1stofmonth zt 
			ON a.ap_account_id = zt.ap_account_id 
			AND DATE_TRUNC('month', a.created_at) = DATE_TRUNC('month', zt.created_at)
			AND zt.signup_hostcountry = 'global'
			AND zt.zip_tier = 'ZipCrew'
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE 
		a.created_at >= '2021-08-01' AND a.created_at < DATE_TRUNC('day', NOW())
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
		AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND a.symbol NOT IN ('TST1','TST2')
--		AND a.ap_account_id = 143639
	ORDER BY 1 DESC 
)	, aum_snapshot AS (
	SELECT 
		DATE_TRUNC('month', created_at)::DATE created_at
		, signup_hostcountry
		, ap_account_id
		, is_pcs
		, CASE WHEN symbol IN ('BTC','ETH','GOLD','LTC','USDC','USDT') THEN 'zipup_coin' 
				WHEN symbol = 'ZMT' THEN 'ZMT' 
				ELSE 'other' END AS asset_group
		, CASE WHEN ziplock_amount >= 0 THEN 1 ELSE 
				(CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) 
				AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
				THEN 1 ELSE 0 END)
				END AS is_zipup_amount
		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0)) total_aum_usd
	FROM 
		base 
	WHERE 
		is_nominee = FALSE
	GROUP BY 
		1,2,3,4,5,6
	ORDER BY 
		1 
)
SELECT 
	created_at
	, signup_hostcountry
	, is_pcs
	, is_zipup_amount
	, COUNT(DISTINCT CASE WHEN z_wallet_amount_usd > 0 OR ziplock_amount_usd > 0 THEN ap_account_id END) user_count
	, COALESCE (SUM( CASE WHEN asset_group = 'ZMT' THEN z_wallet_amount_usd END), 0) AS zipup_zmt_usd
	, COALESCE (SUM( CASE WHEN asset_group = 'zipup_coin' THEN z_wallet_amount_usd END), 0) AS zipup_nozmt_usd
	, COALESCE (SUM( CASE WHEN asset_group = 'ZMT' THEN ziplock_amount_usd END), 0) AS zlock_zmt_usd
	, COALESCE (SUM( CASE WHEN asset_group = 'zipup_coin' THEN ziplock_amount_usd END), 0) AS zlock_nozmt_usd
	, COALESCE (SUM( CASE WHEN asset_group = 'other' THEN z_wallet_amount_usd END), 0) AS other_usd
FROM aum_snapshot
GROUP BY 1,2,3,4
;

SELECT max(created_at), max(inserted_at), max(updated_at)
FROM zip_up_service_public.balance_snapshots