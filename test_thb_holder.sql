DROP TABLE IF EXISTS warehouse.bo_testing.thb_holder_20220621;

CREATE TABLE IF NOT EXISTS warehouse.bo_testing.thb_holder_20220621
(
	created_at				DATE
	, email					varchar(255)
	, vip_tier				varchar(255)
	, ap_account_id			integer
	, signup_hostcountry	varchar(255)
	, symbol				varchar(255)
	, trade_wallet_amount	NUMERIC
	, trade_wallet_amount_usd	NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_thb_holder_20220621 ON warehouse.bo_testing.thb_holder_20220621
(created_at, email, symbol);


CREATE TEMP TABLE IF NOT EXISTS tmp_thb_holder_20220621 AS
(
	WITH coin_base AS (
		SELECT 
			DISTINCT UPPER(SPLIT_PART(product_id,'.',1)) symbol
			, started_at effective_date
			, ended_at expired_date
		FROM zip_up_service_public.interest_rates
		ORDER BY 1
	)	, zipup_coin AS (
		SELECT 
			DISTINCT
			symbol
			, (CASE WHEN effective_date < '2022-03-22' THEN '2018-01-01' ELSE effective_date END)::DATE AS effective_date
			, (CASE WHEN expired_date IS NULL THEN COALESCE( LEAD(effective_date) OVER(PARTITION BY symbol),'2999-12-31') ELSE expired_date END)::DATE AS expired_date
		FROM coin_base 
		ORDER BY 3,2
	)	, base AS (
		SELECT 
			a.created_at::DATE 
			, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
			, a.ap_account_id , up.email 
		-- filter nominee accounts from users_mapping
			, CASE WHEN a.created_at < '2022-05-05' THEN  
				( CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (496001))
				THEN TRUE ELSE FALSE END)
				ELSE
				( CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121 ,496001))
				THEN TRUE ELSE FALSE END)
				END AS is_nominee 
		-- filter asset_manager account
			, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
		-- zipup subscribe status to identify zipup amount
			, (CASE WHEN u.signup_hostcountry = 'TH' THEN
				(CASE WHEN a.created_at < '2022-05-08' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
				WHEN u.signup_hostcountry = 'ID' THEN
				(CASE WHEN a.created_at < '2022-07-04' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
				WHEN u.signup_hostcountry IN ('AU','global') THEN
				(CASE WHEN a.created_at < '2022-06-29' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
				END)::DATE AS zipup_subscribed_at
			, CASE WHEN ult.tier_name IS NULL THEN 'no_zmt' ELSE ult.tier_name END AS vip_tier
			, a.symbol
			, CASE WHEN a.symbol = 'ZMT' THEN TRUE 
					WHEN zc.symbol IS NOT NULL THEN TRUE 
					ELSE FALSE END AS zipup_coin 
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
			LEFT JOIN 
				analytics_pii.users_pii up 
				ON u.user_id = up.user_id 
			LEFT JOIN 
				zipup_coin zc 
				ON a.symbol = zc.symbol
				AND a.created_at >= zc.effective_date
				AND a.created_at < zc.expired_date
			LEFT JOIN 
				analytics.rates_master r 
				ON a.symbol = r.product_1_symbol
				AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
			LEFT JOIN 
				warehouse.zip_up_service_public.user_settings s
				ON u.user_id = s.user_id 
			LEFT JOIN 
				zip_lock_service_public.user_loyalty_tiers ult 
				ON u.user_id = ult.user_id 
		WHERE 
			a.created_at >= '2022-06-01' --AND a.created_at < '2022-06-01'
		-- exclude test products
			AND a.symbol NOT IN ('TST1','TST2')
		    AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
			AND u.signup_hostcountry IN ('TH','ID','AU','global')
			AND a.symbol = 'THB'
		ORDER BY 1 DESC 
	)	, aum_snapshot AS (
		SELECT 
			DATE_TRUNC('day', b.created_at)::DATE created_at
			, b.signup_hostcountry
			, b.ap_account_id
			, b.email
			, b.vip_tier
			, b.symbol asset_group
			, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
			, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
			, SUM( COALESCE (ziplock_amount, 0)) ziplock_amount
			, SUM( COALESCE (zlaunch_amount, 0)) zlaunch_amount
			, SUM( COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
						THEN
							(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount, 0) + COALESCE (z_wallet_amount, 0)
									WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount, 0) END)
						END, 0)) AS zwallet_subscribed_amount
			, SUM( COALESCE (trade_wallet_amount, 0) + COALESCE (z_wallet_amount, 0) 
						+ COALESCE (ziplock_amount, 0) + COALESCE (zlaunch_amount, 0)) total_coin_amount
			, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
			, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
			, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
			, SUM( COALESCE (zlaunch_amount_usd, 0)) zlaunch_amount_usd
			, SUM( COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND b.created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
						THEN
							(CASE 	WHEN b.created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
									WHEN b.created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
						END, 0)) AS zipup_subscribed_usd
			, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) 
						+ COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0)) total_aum_usd
		FROM 
			base b
		WHERE 
			is_asset_manager = FALSE AND is_nominee = FALSE
		GROUP BY 
			1,2,3,4,5,6
		ORDER BY 
			1 
	)
	SELECT 
		created_at::DATE
		, email 
		, vip_tier
		, ap_account_id
		, signup_hostcountry
		, asset_group
		, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
	FROM aum_snapshot
	GROUP BY
		1,2,3,4,5,6
	ORDER BY 6 DESC 
);

INSERT INTO warehouse.bo_testing.thb_holder_20220621
(SELECT * FROM tmp_thb_holder_20220621);

DROP TABLE IF EXISTS tmp_thb_holder_20220621;

