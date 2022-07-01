-- pcs global definition: ZMT lock >= 20,000 unit (VIP4) OR total AUM USD >= 50,000 USD

DROP TABLE IF EXISTS warehouse.mappings.commercial_pcs_gl_1st_of_month;

CREATE TABLE IF NOT EXISTS warehouse.mappings.commercial_pcs_gl_1st_of_month
(
	id						SERIAL PRIMARY KEY 
	, created_at	 		DATE
	, signup_hostcountry 	VARCHAR(255)
	, ap_account_id		 	INTEGER 
	, zmt_amount			NUMERIC
	, total_aum_usd			NUMERIC
	, status				VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_commercial_pcs_gl_1st_of_month ON warehouse.mappings.commercial_pcs_gl_1st_of_month
(created_at, ap_account_id);

DROP TABLE IF EXISTS tmp_commercial_pcs_gl_1st_of_month;

CREATE TEMP TABLE IF NOT EXISTS tmp_commercial_pcs_gl_1st_of_month AS 
(
-- pcs global definition: 20K ZMT (all wallets) OR 50K USD (total AUM)
	WITH base AS 
	(
		SELECT 
			a.created_at 
			, u.signup_hostcountry
			, a.ap_account_id  
			, a.symbol 
			, (trade_wallet_amount + z_wallet_amount + ziplock_amount + zlaunch_amount) total_unit
			, CASE	WHEN r.product_type = 1 
				THEN (trade_wallet_amount + z_wallet_amount + ziplock_amount + zlaunch_amount) * 1/r.price 
					WHEN r.product_type = 2 
				THEN (trade_wallet_amount + z_wallet_amount + ziplock_amount + zlaunch_amount) * r.price
					END AS total_aum_usd
		FROM 
			analytics.wallets_balance_eod a 
		-- get country and join with pii data
			LEFT JOIN 
				analytics.users_master u 
				ON a.ap_account_id = u.ap_account_id 
		-- get pii data 
			LEFT JOIN 
				analytics_pii.users_pii up 
				ON u.user_id = up.user_id 
		-- coin prices and exchange rates (USD)
			LEFT JOIN 
				analytics.rates_master r 
				ON a.symbol = r.product_1_symbol
				AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
		WHERE 
			a.created_at >= '2022-01-01' AND a.created_at < DATE_TRUNC('day', NOW())::DATE
			AND u.signup_hostcountry IN ('global')
		-- filter accounts from users_mapping
			AND a.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		-- snapshot by 1st of month 
			AND a.created_at = DATE_TRUNC('month', a.created_at)
		-- exclude test products
			AND a.symbol NOT IN ('TST1','TST2')
		ORDER BY 1 DESC 
	)	
		, aum_snapshot AS 
	(
		SELECT 
			DATE_TRUNC('day', created_at)::DATE created_at
			, signup_hostcountry
			, ap_account_id
			, COALESCE( SUM( CASE WHEN symbol = 'ZMT' THEN total_unit END), 0) zmt_amount
			, SUM( COALESCE (total_aum_usd, 0)) total_aum_usd
		FROM 
			base 
		GROUP BY 
			1,2,3
		ORDER BY 
			1 
	)
	SELECT 
		*
		, CASE WHEN zmt_amount >= 20000 OR total_aum_usd >= 50000 THEN 'pcs' ELSE 'near_pcs' END AS status
	FROM 
		aum_snapshot
	WHERE 
		zmt_amount >= 20000
		OR total_aum_usd >= 30000
);

INSERT INTO warehouse.mappings.commercial_pcs_gl_1st_of_month (created_at, signup_hostcountry, ap_account_id, zmt_amount, total_aum_usd, status)
( SELECT * FROM tmp_commercial_pcs_gl_1st_of_month );

DROP TABLE IF EXISTS tmp_commercial_pcs_gl_1st_of_month;



-- pcs datamart
SELECT 
--	*,
	um.created_at::DATE register_date
	, um.verification_approved_at::DATE verified_date 
	, um.user_id 
	, um.ap_account_id 
	, um.signup_hostcountry 
	, um.is_zipup_subscribed 
	, NOW()::DATE - uf.inserted_at::DATE pcs_lifetime_day
	, uf.updated_at pcs_tagged_at
	, CASE WHEN f.code IN ('PCS', 'CORPORATE') THEN
			(CASE WHEN (NOW()::DATE - uf.inserted_at::DATE) <= cnl.timeframe_day::INT 
					AND (sum_deposit_amount_usd - sum_withdraw_amount_usd) < cnl.min_net_deposit_usd::NUMERIC  THEN 'trial_pcs'
				WHEN (NOW()::DATE - uf.inserted_at::DATE) <= cnl.timeframe_day::INT 
					AND (sum_deposit_amount_usd - sum_withdraw_amount_usd) >= cnl.min_net_deposit_usd::NUMERIC  THEN 'new_pcs'
				WHEN (NOW()::DATE - uf.inserted_at::DATE) > cnl.timeframe_day::INT 
					AND (sum_deposit_amount_usd - sum_withdraw_amount_usd) >= cnl.min_net_deposit_usd::NUMERIC  THEN 'existing_pcs'
				WHEN (NOW()::DATE - uf.inserted_at::DATE) > cnl.timeframe_day::INT 
					AND (sum_deposit_amount_usd - sum_withdraw_amount_usd) < cnl.min_net_deposit_usd::NUMERIC  THEN 'at_risk_pcs'
		ELSE 'unknown' END) END AS pcs_status
	, CASE 	WHEN f.code IS NULL THEN 
			(CASE WHEN (sum_deposit_amount_usd - sum_withdraw_amount_usd) >= cnl2.min_net_deposit_usd::NUMERIC 
					AND (sum_deposit_amount_usd - sum_withdraw_amount_usd) < cnl2.max_net_deposit_usd::NUMERIC 
			THEN 'near_pcs' ELSE 'non_pcs' END)
		ELSE f.code END AS near_pcs_status
	, CASE WHEN ult.tier_name IS NULL THEN 'no_zmt' ELSE ult.tier_name END AS vip_tier
	, ult.zmt_balance zmt_lock_amount
	, SUM(CASE WHEN d.created_at >= NOW()::DATE - '30 day'::INTERVAL THEN d.sum_usd_deposit_amount END) l30d_deposit_usd
	, SUM(CASE WHEN d.created_at >= NOW()::DATE - '30 day'::INTERVAL THEN d.sum_usd_withdraw_amount END) l30d_withdraw_usd
	, SUM(CASE WHEN d.created_at >= NOW()::DATE - '30 day'::INTERVAL THEN d.sum_usd_trade_amount  END) l30d_trade_usd
FROM analytics.users_master um 
	LEFT JOIN user_app_public.user_features uf 
		ON um.user_id = uf.user_id 
	LEFT JOIN user_app_public.features f 
		ON uf.feature_id = f.id 
	LEFT JOIN analytics_pii.users_pii up 
		ON um.user_id = up.user_id 
	LEFT JOIN mappings.commercial_nearpcs_logic cnl
		ON um.signup_hostcountry = cnl.country 
		AND cnl.pcs_type = 'PCS'
	LEFT JOIN mappings.commercial_nearpcs_logic cnl2
		ON um.signup_hostcountry = cnl2.country 
		AND cnl2.pcs_type != 'PCS'
	LEFT JOIN zip_lock_service_public.user_loyalty_tiers ult 
		ON um.user_id = ult.user_id 
	LEFT JOIN reportings_data.dm_user_transactions_dwt_daily d
		ON um.user_id = d.user_id 
	LEFT JOIN reportings_data.dm_zw_daily_transations zd 
WHERE 
	CASE WHEN um.signup_hostcountry = 'TH' THEN (sum_deposit_amount_usd - sum_withdraw_amount_usd) > cnl2.min_net_deposit_usd::NUMERIC 
			WHEN um.signup_hostcountry = 'ID' THEN (sum_deposit_amount_usd - sum_withdraw_amount_usd) > cnl2.min_net_deposit_usd::NUMERIC 
			WHEN um.signup_hostcountry = 'AU' THEN (sum_deposit_amount_usd - sum_withdraw_amount_usd) > cnl2.min_net_deposit_usd::NUMERIC 
			WHEN um.signup_hostcountry = 'global' THEN (sum_deposit_amount_usd - sum_withdraw_amount_usd) > cnl2.min_net_deposit_usd::NUMERIC 
			END
	AND (f.code NOT IN ('TEST','INTERNAL') OR f.code IS NULL)
	AND um.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
--	AND um.user_id = '01EYKWGFG45WE55BKWHGX1KE2R'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
;



-- pcs aum 
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
		, dpu.signup_hostcountry 
		, a.ap_account_id 
	-- zipup subscribe status to identify zipup amount
		, (CASE WHEN u.signup_hostcountry = 'TH' THEN
			(CASE WHEN a.created_at < '2022-05-24' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			WHEN u.signup_hostcountry = 'ID' THEN
			(CASE WHEN a.created_at < '2022-07-04' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			WHEN u.signup_hostcountry IN ('AU','global') THEN
			(CASE WHEN a.created_at < '2022-06-29' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			END)::DATE AS zipup_subscribed_at
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
		RIGHT JOIN 
			bo_testing.dm_pcs_user dpu 
			ON a.ap_account_id = dpu.ap_account_id 
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
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
		LEFT JOIN 
			warehouse.zip_up_service_public.user_settings s
			ON u.user_id = s.user_id 
	WHERE 
		a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
	-- exclude test products
		AND a.symbol NOT IN ('TST1','TST2')
	ORDER BY 1 DESC 
)
SELECT 
	DATE_TRUNC('day', b.created_at)::DATE balanced_at
	, b.signup_hostcountry
	, ap_account_id
	, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) 
				+ COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0)) asset_on_platform_usd
	, SUM( CASE WHEN symbol = 'ZMT' THEN COALESCE (trade_wallet_amount_usd, 0) END) zmt_trade_wallet_amount_usd
	, SUM( CASE WHEN symbol = 'ZMT' THEN COALESCE (z_wallet_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0) END) zmt_z_wallet_amount_usd
	, SUM( CASE WHEN symbol != 'ZMT' THEN COALESCE (trade_wallet_amount_usd, 0) END) non_zmt_trade_wallet_amount_usd
	, SUM( CASE WHEN symbol != 'ZMT' THEN COALESCE (z_wallet_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0) END) non_zmt_z_wallet_amount_usd
	, SUM( CASE WHEN symbol = 'ZMT' THEN
			COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND b.created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
				THEN
					(CASE 	WHEN b.created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
							WHEN b.created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
				END, 0) END) AS zmt_zipup_usd
	, SUM( CASE WHEN symbol != 'ZMT' THEN
			COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND b.created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
				THEN
					(CASE 	WHEN b.created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
							WHEN b.created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
				END, 0) END) AS non_zmt_zipup_usd
FROM 
	base b
GROUP BY 
	1,2,3
ORDER BY 3
;


WITH base AS (
	SELECT 
		register_date
		, dpu.register_date
		, dpu.verified_at
		, dpu.user_id
		, dpu.ap_account_id
		, dpu.signup_hostcountry
		, dpu.is_zipup_subscribed
		, dpu.pcs_lifetime_day
		, dpu.pcs_tagged_at
		, CASE WHEN dpu.pcs_status IS NULL THEN dpu.near_pcs_status ELSE dpu.pcs_status END AS pcs_status	
		, dpu.vip_tier
		, dpu.zmt_lock_amount
		, dpu.l30d_deposit_usd
		, dpu.l30d_withdraw_usd
		, dpu.l30d_trade_usd
		, dpu.l30d_transfer_to_zwallet_usd
		, dpu.l30d_withdraw_from_zwallet_usd
		, dpu.balanced_at
		, dpu.asset_on_platform_usd
		, dpu.zmt_trade_wallet_amount_usd
		, dpu.zmt_z_wallet_amount_usd
		, dpu.non_zmt_trade_wallet_amount_usd
		, dpu.non_zmt_z_wallet_amount_usd
		, dpu.zmt_zipup_usd
		, dpu.non_zmt_zipup_usd
		, up.first_name 
		, up.last_name 
		, up.email 
		, up.mobile_number 
		, up.dob::DATE 
	FROM bo_testing.dm_pcs_user dpu 
		LEFT JOIN 
			analytics_pii.users_pii up 
			ON dpu.user_id = up.user_id 
