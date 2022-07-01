--	Avg trading amount for the first 30 days after the non-whale users have first deposit
WITH base AS (
	SELECT
		DATE_TRUNC('day', t.created_at) traded_date
		, DATE_TRUNC('day', u.first_deposit_at) first_deposit_at
		, t.signup_hostcountry 
		, t.ap_account_id 
		, COUNT(DISTINCT t.order_id) order_count
		, SUM(COALESCE (t.amount_usd, 0)) trade_volume_usd
	FROM analytics.trades_master t
		LEFT JOIN analytics.users_master u
			ON t.ap_account_id = u.ap_account_id 
	WHERE 
		t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.commercial_is_whale)
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
		AND t.created_at < DATE_TRUNC('day', NOW())
	GROUP BY 1,2,3,4
)
SELECT 
	DATE_TRUNC('month', first_deposit_at) first_deposit_month
	, signup_hostcountry
	, ap_account_id 
	, COUNT(DISTINCT CASE WHEN traded_date <= first_deposit_at + '30 day' THEN traded_date END) trade_day_count
	, SUM(CASE WHEN traded_date <= first_deposit_at + '30 day' THEN order_count END) AS order_count_30d
	, SUM(CASE WHEN traded_date <= first_deposit_at + '30 day' THEN trade_volume_usd END) AS trade_volume_usd_30d
FROM base 
GROUP BY 1,2,3
ORDER BY 1
;


--	Avg trading amount for the first 30 days after the non-whale users have KYCed
WITH base AS (
	SELECT
		DATE_TRUNC('day', t.created_at) traded_date
		, DATE_TRUNC('day', u.onfido_completed_at) verified_date
		, t.signup_hostcountry 
		, t.ap_account_id 
		, COUNT(DISTINCT t.order_id) order_count
		, SUM(COALESCE (t.amount_usd, 0)) trade_volume_usd
	FROM analytics.trades_master t
		LEFT JOIN analytics.users_master u
			ON t.ap_account_id = u.ap_account_id 
	WHERE 
		t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.commercial_is_whale)
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
		AND t.created_at < DATE_TRUNC('day', NOW())
	GROUP BY 1,2,3,4
)
SELECT 
	DATE_TRUNC('month', verified_date) verified_month
	, signup_hostcountry
	, ap_account_id 
	, COUNT(DISTINCT CASE WHEN traded_date <= verified_date + '30 day' THEN traded_date END) trade_day_count
	, SUM(CASE WHEN traded_date <= verified_date + '30 day' THEN order_count END) AS order_count_30d
	, SUM(CASE WHEN traded_date <= verified_date + '30 day' THEN trade_volume_usd END) AS trade_volume_usd_30d
FROM base 
GROUP BY 1,2,3
ORDER BY 1
;


--	Avg ZipUp + ZipLock amount for the first 30 days after the non-whale users have first deposit
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
		, DATE_TRUNC('day', u.onfido_completed_at) verified_date
		, DATE_TRUNC('day', u.first_deposit_at) first_deposit_at
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
		a.created_at < DATE_TRUNC('day', NOW()) 
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
		AND a.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.commercial_is_whale)
	--	AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND a.symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH')
	ORDER BY 1 DESC 
)	, aum_snapshot AS (
	SELECT 
		created_at balanced_at
		, verified_date
		, signup_hostcountry
		, ap_account_id
		, SUM( CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at)
				AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
				THEN 
						(CASE WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
								WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
				END) AS zipup_amount_usd
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
	FROM 
		base 
	WHERE 
		is_asset_manager = FALSE AND is_nominee = FALSE
	GROUP BY 
		1,2,3,4
	ORDER BY 
		1
)	, zipup_ziplock_30d AS (
	SELECT 
		DATE_TRUNC('month', verified_date) verified_month
		, ap_account_id 
		, signup_hostcountry
		, COUNT(DISTINCT CASE WHEN balanced_at <= verified_date + '30 day' THEN balanced_at END) balance_day_count
		, SUM(CASE WHEN balanced_at <= verified_date + '30 day' THEN zipup_amount_usd END) AS zipup_amount_usd_30d_kyc
		, SUM(CASE WHEN balanced_at <= verified_date + '30 day' THEN ziplock_amount_usd END) AS ziplock_amount_usd_30d_kyc
	FROM aum_snapshot
	GROUP BY 1,2,3
)
SELECT 
	*
	, zipup_amount_usd_30d_kyc/ balance_day_count avg_zipup_kyc_30d
	, ziplock_amount_usd_30d_kyc/ balance_day_count avg_ziplock_kyc_30d
FROM zipup_ziplock_30d
;


--	Avg ZipUp + ZipLock amount for the first 30 days after the non-whale users have KYCed
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
		, DATE_TRUNC('day', u.onfido_completed_at) verified_date
		, DATE_TRUNC('day', u.first_deposit_at) first_deposit_at
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
		a.created_at < DATE_TRUNC('day', NOW()) 
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
		AND a.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.commercial_is_whale)
	--	AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND a.symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH')
	ORDER BY 1 DESC 
)	, aum_snapshot AS (
	SELECT 
		created_at balanced_at
		, first_deposit_at
		, signup_hostcountry
		, ap_account_id
		, SUM( CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at)
				AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
				THEN 
						(CASE WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
								WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
				END) AS zipup_amount_usd
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
	FROM 
		base 
	WHERE 
		is_asset_manager = FALSE AND is_nominee = FALSE
	GROUP BY 
		1,2,3,4
	ORDER BY 
		1
)	, zipup_ziplock_30d AS (
	SELECT 
		DATE_TRUNC('month', first_deposit_at) first_deposit_month
		, ap_account_id 
		, COUNT(DISTINCT CASE WHEN balanced_at <= first_deposit_at + '30 day' THEN balanced_at END) balance_day_count
		, SUM(CASE WHEN balanced_at <= first_deposit_at + '30 day' THEN zipup_amount_usd END) AS zipup_amount_usd_30d_deposit
		, SUM(CASE WHEN balanced_at <= first_deposit_at + '30 day' THEN ziplock_amount_usd END) AS ziplock_amount_usd_30d_deposit
	FROM aum_snapshot
	GROUP BY 1,2
)
SELECT 
	*
	, zipup_amount_usd_30d_deposit/ balance_day_count avg_zipup_deposit_30d
	, zipup_amount_usd_30d_deposit/ balance_day_count avg_ziplock_deposit_30d
FROM zipup_ziplock_30d
;


