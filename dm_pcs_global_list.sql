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
WITH user_features AS (
	SELECT 
		*
		, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY updated_at) row_ 
	FROM user_app_public.user_features uf 
)
SELECT 
	NOW()::DATE inserted_at
	, um.created_at::DATE register_date
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
--	, SUM(CASE WHEN d.created_at >= NOW()::DATE - '30 day'::INTERVAL THEN zd.transfer_to_zwallet_usd END) l30d_transfer_to_zwallet_usd
--	, SUM(CASE WHEN d.created_at >= NOW()::DATE - '30 day'::INTERVAL THEN zd.withdraw_from_zwallet_usd END) l30d_withdraw_from_zwallet_usd
FROM analytics.users_master um 
	LEFT JOIN user_features uf 
		ON um.user_id = uf.user_id 
		AND row_ = 1
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
	LEFT JOIN reportings_data.dm_user_transactions_dwt_hourly d
		ON um.user_id = d.user_id 
	LEFT JOIN bo_testing.dm_zw_hourly_transations zd 
		ON um.ap_account_id = zd.ap_account_id 
		AND zd.product_1_symbol IN (SELECT DISTINCT symbol FROM mappings.zip_up_rates_master zurm WHERE ended_at IS NULL)
WHERE 
	CASE WHEN um.signup_hostcountry = 'TH' THEN (sum_deposit_amount_usd - sum_withdraw_amount_usd) > cnl2.min_net_deposit_usd::NUMERIC 
			WHEN um.signup_hostcountry = 'ID' THEN (sum_deposit_amount_usd - sum_withdraw_amount_usd) > cnl2.min_net_deposit_usd::NUMERIC 
			WHEN um.signup_hostcountry = 'AU' THEN (sum_deposit_amount_usd - sum_withdraw_amount_usd) > cnl2.min_net_deposit_usd::NUMERIC 
			WHEN um.signup_hostcountry = 'global' THEN (sum_deposit_amount_usd - sum_withdraw_amount_usd) > cnl2.min_net_deposit_usd::NUMERIC 
			END
	AND (f.code NOT IN ('TEST','INTERNAL') OR f.code IS NULL)
	AND um.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
--	AND um.user_id = '01EYKWGFG45WE55BKWHGX1KE2R'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13



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


/*
 * SQL Code Owner : Jesse
 * Description: generate PCS users for all countries, consolidation in 1 Gsheet
 * Link/URL to the target Google sheet : https://docs.google.com/spreadsheets/d/1dHKjnulBW2H45kOcUMsgcPQkbgp9fYyeFpths2IWEx8/edit#gid=0
 * Sheet name: PCS Tracking and Reporting
 * Tab name : DO_NOT_EDIT_RM_Daily_Monitoring  
 * Timing of run: 11:00 GMT+7 Daily
 */

WITH seg_base AS (
	SELECT *
	FROM mappings.commercial_customer_segmentation_th
	UNION ALL
	SELECT *
	FROM mappings.commercial_customer_segmentation_id
	UNION ALL
	SELECT *
	FROM mappings.commercial_customer_segmentation_au
	UNION ALL
	SELECT *
	FROM mappings.commercial_customer_segmentation_global
)--	, base AS (
SELECT 
	CASE WHEN dpu.balanced_at IS NULL THEN '1970-01-01' ELSE dpu.balanced_at END AS balanced_at
	, CASE WHEN cp.pcs_rm IS NULL THEN 'N/A' ELSE cp.pcs_rm END AS pcs_rm
	, dpu.user_id
	, dpu.ap_account_id
	, dpu.signup_hostcountry
--	, CASE WHEN sb.customer_segment IS NULL THEN 'N/A' ELSE sb.customer_segment END AS customer_segment
	, dpu.register_date
	, dpu.verified_at
	, CASE WHEN up.email  IS NULL THEN 'N/A' ELSE up.email END AS email 
	, CASE WHEN up.first_name IS NULL THEN 'N/A' ELSE up.first_name END AS first_name 
	, CASE WHEN up.last_name  IS NULL THEN 'N/A' ELSE up.last_name END AS last_name 
	, CASE WHEN up.mobile_number IS NULL THEN 'N/A' ELSE up.mobile_number END AS mobile_number 
	, CASE WHEN up.dob::DATE IS NULL THEN '1970-01-01'::DATE ELSE up.dob::DATE END AS dob
	, CASE WHEN dpu.pcs_tagged_at IS NULL THEN '1970-01-01'::DATE ELSE dpu.pcs_tagged_at END AS pcs_tagged_at
	, CASE WHEN dpu.pcs_status IS NULL THEN dpu.near_pcs_status ELSE dpu.pcs_status END AS pcs_status	
	, dpu.vip_tier
	, dpu.is_zipup_subscribed
	, COALESCE (dpu.l30d_deposit_count, 0) l30d_deposit_count
	, COALESCE (dpu.l30d_deposit_usd, 0) l30d_deposit_usd
	, COALESCE (dpu.l30d_withdraw_count, 0) l30d_withdraw_count
	, COALESCE (dpu.l30d_withdraw_usd, 0) l30d_withdraw_usd
	, COALESCE (dpu.zmt_release_this_month , 0) zmt_release_this_month
	, COALESCE (dpu.zmt_release_next_month , 0) zmt_release_next_month
	, COALESCE (dpu.asset_on_platform_usd, 0) asset_on_platform_usd
	, COALESCE (dpu.zmt_trade_wallet_amount_usd, 0) zmt_trade_wallet_amount_usd
	, COALESCE (dpu.zmt_z_wallet_amount_usd, 0) zmt_z_wallet_amount_usd
	, COALESCE (dpu.zmt_zipup_usd, 0) zmt_zipup_usd
	, COALESCE (dpu.zmt_lock_amount, 0) zmt_lock_amount
	, COALESCE (dpu.non_zmt_trade_wallet_amount_usd, 0) non_zmt_trade_wallet_amount_usd
	, COALESCE (dpu.non_zmt_z_wallet_amount_usd, 0) non_zmt_z_wallet_amount_usd
	, COALESCE (dpu.non_zmt_zipup_usd, 0) non_zmt_zipup_usd
	, COALESCE (dpu.l30d_transfer_to_zwallet_usd, 0) zipup_deposit_transfer
	, COALESCE (dpu.l30d_withdraw_from_zwallet_usd, 0) zipup_withdraw_transfer
	, COALESCE (dpu.f1m_trade_usd , 0) f1m_trade_usd
	, COALESCE (dpu.l30d_trade_count , 0) l30d_trade_count
	, COALESCE (dpu.l30d_trade_usd , 0) l30d_trade_usd
FROM reportings_data.dm_pcs_user dpu 
-- customer segment as of Mar 31st 2022
	LEFT JOIN seg_base sb 
		ON dpu.ap_account_id = sb.ap_account_id::INT 
	LEFT JOIN 
		analytics_pii.users_pii up 
		ON dpu.user_id = up.user_id 
	LEFT JOIN 
		mappings.commercial_pcs_rm cp 
		ON up.email = cp.pcs_email 
WHERE dpu.data_as_of = NOW()::DATE
--AND dpu.user_id = '01G2RHZ8D1P86SH8E07YHB739H'

-- validate data 
)
SELECT 
	balanced_at
	, user_id
--	, pcs_status 
	, COUNT(user_id) 
	, COUNT(DISTINCT user_id)
FROM base 
GROUP BY 1,2
ORDER BY 3 DESC 
;


SELECT 
	*
FROM user_app_public.user_features uf 
	LEFT JOIN user_app_public.features f 
	ON uf.feature_id = f.id 
WHERE user_id IN ('01EGF41H4AD8B7KQR1KG5WMVST','01EY9TT17RNGFZW3VQPF5F7VZ4','01EVG91MZ6FEF68W0RFRHKTJW2','01EQ2NGDR7JRN453TB0J7NB2WG','01FZ73JGGT4V0F0JDD5GWS3EJ6')


SELECT 
	DATE_TRUNC('month', d.created_at)::DATE transact_date
	, 'monthly' timeframe
	, dpu.balanced_at 
	, dpu.signup_hostcountry 
	, dpu.pcs_status  
	, COUNT(DISTINCT dpu.user_id) user_count
	, SUM(d.sum_usd_deposit_amount/ 1000000.0)  deposit_usd_m 
	, SUM(d.sum_usd_withdraw_amount/ 1000000.0) withdraw_usd_m 
	, SUM(d.sum_usd_deposit_amount/ 1000000.0) - SUM(d.sum_usd_withdraw_amount/ 1000000.0) net_deposit_usd_m
FROM reportings_data.dm_pcs_user dpu 
	LEFT JOIN 
		reportings_data.dm_user_transactions_dwt_hourly d
		ON dpu.ap_account_id = d.ap_account_id 
		AND d.created_at >= DATE_TRUNC('month', NOW()) - '6 month'::INTERVAL
WHERE 
	dpu.pcs_status IS NOT NULL
	AND dpu.data_as_of = NOW()::DATE
GROUP BY 1,2,3,4,5
UNION ALL 
SELECT 
	DATE_TRUNC('week', d.created_at)::DATE transact_date
	, 'weekly' timeframe
	, dpu.balanced_at 
	, dpu.signup_hostcountry 
	, dpu.pcs_status  
	, COUNT(DISTINCT dpu.user_id) user_count
	, SUM(d.sum_usd_deposit_amount/ 1000000.0)  deposit_usd_m 
	, SUM(d.sum_usd_withdraw_amount/ 1000000.0) withdraw_usd_m 
	, SUM(d.sum_usd_deposit_amount/ 1000000.0) - SUM(d.sum_usd_withdraw_amount/ 1000000.0) net_deposit_usd_m
FROM reportings_data.dm_pcs_user dpu 
	LEFT JOIN 
		reportings_data.dm_user_transactions_dwt_hourly d
		ON dpu.ap_account_id = d.ap_account_id 
		AND d.created_at >= DATE_TRUNC('month', NOW()) - '6 month'::INTERVAL
WHERE 
	dpu.pcs_status IS NOT NULL 
	AND dpu.data_as_of = NOW()::DATE
GROUP BY 1,2,3,4,5
;


