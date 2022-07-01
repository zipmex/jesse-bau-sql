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
		, CASE WHEN a.created_at < '2022-09-29' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END AS zipup_subscribed_at
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
	-- get country and join with pii data
		LEFT JOIN 
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
	-- coin prices and exchange rates (USD)
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
		LEFT JOIN 
			warehouse.zip_up_service_public.user_settings s
			ON u.user_id = s.user_id 
	WHERE 
		a.created_at >= '2021-01-01' AND a.created_at < DATE_TRUNC('day', NOW())::DATE
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
	-- snapshot by end of month or yesterday
		AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
	-- exclude test products
		AND a.symbol IN ('BTC','ETH','GOLD','LTC','USDC','USDT','ZMT') 
	ORDER BY 1 DESC 
)	, aum_snapshot AS (
	SELECT 
		DATE_TRUNC('month', created_at)::DATE created_at
		, signup_hostcountry
		, ap_account_id
		, CASE WHEN symbol = 'ZMT' THEN 1 ELSE 0 END AS asset_group
		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		, SUM( COALESCE (zlaunch_amount_usd, 0)) zlaunch_amount_usd
		, SUM( COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
					THEN
						(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
								WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
					END, 0)) AS interest_zipup_usd
		, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
				+ COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0)) total_amount_usd
	FROM 
		base 
	WHERE 
		is_asset_manager = FALSE AND is_nominee = FALSE
	GROUP BY 
		1,2,3,4
	ORDER BY 
		1 
)	, aum_zmt AS (
	SELECT 
		created_at
		, signup_hostcountry
		, ap_account_id 
		, SUM( CASE WHEN asset_group = 1 THEN interest_zipup_usd END) zmt_zipup_usd
		, SUM( CASE WHEN asset_group = 0 THEN interest_zipup_usd END) non_zmt_zipup_usd
		, SUM( CASE WHEN asset_group = 1 THEN ziplock_amount_usd END) zmt_ziplock_usd
		, SUM( CASE WHEN asset_group = 0 THEN ziplock_amount_usd END) non_zmt_ziplock_usd
		, SUM( CASE WHEN asset_group = 1 THEN zlaunch_amount_usd END) zmt_zlaunch_usd
		, SUM( CASE WHEN asset_group = 0 THEN zlaunch_amount_usd END) non_zmt_zlaunch_usd
		, SUM( CASE WHEN asset_group = 1 THEN total_amount_usd END) total_zmt_usd
	FROM aum_snapshot
	GROUP BY 1,2,3
)
SELECT 
	created_at
	, signup_hostcountry
	, COUNT(DISTINCT CASE 
				WHEN (COALESCE( zmt_zipup_usd, 0) + COALESCE( non_zmt_zipup_usd, 0)) >= 1 
				THEN ap_account_id END) AS zipup_total_user_count
	, COUNT(DISTINCT CASE 
				WHEN COALESCE( zmt_zipup_usd, 0) >= 1 AND COALESCE( non_zmt_zipup_usd, 0) >= 1 
				THEN ap_account_id END) AS zipup_mix_user_count
	, COUNT(DISTINCT CASE 
				WHEN COALESCE( zmt_zipup_usd, 0) >= 1 AND COALESCE( non_zmt_zipup_usd, 0) < 1 
				THEN ap_account_id END) AS zipup_zmt_user_count
	, COUNT(DISTINCT CASE 
				WHEN COALESCE( zmt_zipup_usd, 0) < 1 AND COALESCE( non_zmt_zipup_usd, 0) >= 1 
				THEN ap_account_id END) AS zipup_non_zmt_user_count
	, COUNT(DISTINCT CASE 
				WHEN (COALESCE( zmt_ziplock_usd, 0) + COALESCE( non_zmt_ziplock_usd, 0)) > 0 
				THEN ap_account_id END) AS ziplock_total_user_count
	, COUNT(DISTINCT CASE 
				WHEN COALESCE( zmt_ziplock_usd, 0) >= 1 AND COALESCE( non_zmt_ziplock_usd, 0) >= 1 
				THEN ap_account_id END) AS ziplock_mix_user_count
	, COUNT(DISTINCT CASE 
				WHEN COALESCE( zmt_ziplock_usd, 0) >= 1 AND COALESCE( non_zmt_ziplock_usd, 0) < 1 
				THEN ap_account_id END) AS ziplock_zmt_user_count
	, COUNT(DISTINCT CASE 
				WHEN COALESCE( zmt_ziplock_usd, 0) < 1 AND COALESCE( non_zmt_ziplock_usd, 0) >= 1 
				THEN ap_account_id END) AS ziplock_non_zmt_user_count
	, COUNT(DISTINCT CASE 
				WHEN (COALESCE( zmt_zlaunch_usd, 0) + COALESCE( non_zmt_zlaunch_usd, 0)) > 0 
				THEN ap_account_id END) AS zlaunch_total_user_count
	, COUNT(DISTINCT CASE 
				WHEN COALESCE( zmt_zlaunch_usd, 0) > 0 AND COALESCE( non_zmt_zlaunch_usd, 0) > 0 
				THEN ap_account_id END) AS zlaunch_mix_user_count
	, COUNT(DISTINCT CASE 
				WHEN COALESCE( zmt_zlaunch_usd, 0) > 0 AND COALESCE( non_zmt_zlaunch_usd, 0) = 0 
				THEN ap_account_id END) AS zlaunch_zmt_user_count
	, COUNT(DISTINCT CASE 
				WHEN COALESCE( zmt_zlaunch_usd, 0) = 0 AND COALESCE( non_zmt_zlaunch_usd, 0) > 0 
				THEN ap_account_id END) AS zlaunch_non_zmt_user_count
	, COUNT(DISTINCT CASE 
				WHEN COALESCE( total_zmt_usd, 0) > 1 
				THEN ap_account_id END) AS zmt_holder_1usd_count
FROM aum_zmt
GROUP BY
	1,2
;



