-- pcs datamart
DROP TABLE IF EXISTS warehouse.bo_testing.dm_pcs_user;

CREATE TABLE IF NOT EXISTS warehouse.bo_testing.dm_pcs_user
(
	data_as_of					DATE
	, register_date				DATE
	, verified_at				DATE
	, user_id					VARCHAR(255)
	, ap_account_id				INTEGER
	, signup_hostcountry		VARCHAR(255)
	, is_zipup_subscribed		BOOLEAN
	, pcs_lifetime_day			INTEGER
	, pcs_tagged_at				TIMESTAMP
	, pcs_status				VARCHAR(255)
	, near_pcs_status			VARCHAR(255)
	, vip_tier							VARCHAR(255)
	, zmt_lock_amount					NUMERIC
	, l30d_deposit_count				INTEGER
	, l30d_deposit_usd					NUMERIC
	, l30d_withdraw_count				INTEGER
	, l30d_withdraw_usd					NUMERIC
	, l30d_trade_count					INTEGER
	, l30d_trade_usd					NUMERIC
	, f1m_trade_usd						NUMERIC
	, l30d_transfer_to_zwallet_usd		NUMERIC
	, l30d_withdraw_from_zwallet_usd	NUMERIC
	, zmt_release_this_month			NUMERIC
	, zmt_release_next_month			NUMERIC
	, balanced_at						DATE
	, asset_on_platform_usd				NUMERIC
	, zmt_trade_wallet_amount_usd		NUMERIC
	, zmt_z_wallet_amount_usd			NUMERIC
	, zmt_zipup_usd						NUMERIC
	, non_zmt_trade_wallet_amount_usd	NUMERIC
	, non_zmt_z_wallet_amount_usd		NUMERIC
	, non_zmt_zipup_usd					NUMERIC
);

CREATE INDEX IF NOT EXISTS tmp_dm_pcs_user ON warehouse.bo_testing.dm_pcs_user
(ap_account_id, user_id, register_date, verified_at, signup_hostcountry, pcs_status, near_pcs_status, vip_tier);

ALTER TABLE warehouse.bo_testing.dm_pcs_user REPLICA IDENTITY FULL;

CREATE TEMP TABLE IF NOT EXISTS tmp_ziplock_release AS 
(
	WITH base AS (
		SELECT 
			release_datetime 
			, user_id 
			, UPPER(SPLIT_PART(product_id ,'.',1)) symbol
			, lock_days 
			, SUM(amount) locked_amount
		FROM zip_lock_service_public.lock_transactions lt 
		WHERE 
			release_datetime IS NOT NULL 
			AND status = 'completed'
			AND release_datetime >= DATE_TRUNC('month', NOW())
		GROUP BY 1,2,3,4
		ORDER BY 1 DESC 
	)
	SELECT 
		user_id 
		, symbol
		, SUM( CASE WHEN DATE_TRUNC('month', release_datetime) = DATE_TRUNC('month', NOW()) THEN locked_amount END) zmt_release_this_month
		, SUM( CASE WHEN DATE_TRUNC('month', release_datetime) = DATE_TRUNC('month', NOW()) + '1 month'::INTERVAL THEN locked_amount END) zmt_release_next_month
	FROM base 
	WHERE symbol = 'ZMT'
	GROUP BY 1,2
);

CREATE TEMP TABLE IF NOT EXISTS tmp_dm_pcs_user_base AS 
(
	WITH user_features AS (
		SELECT 
			*
			, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY updated_at DESC) row_ 
		FROM user_app_public.user_features uf 
	)
	SELECT 
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
	WHERE 
--		CASE WHEN um.signup_hostcountry = 'TH' THEN (sum_deposit_amount_usd - sum_withdraw_amount_usd) > cnl2.min_net_deposit_usd::NUMERIC 
--				WHEN um.signup_hostcountry = 'ID' THEN (sum_deposit_amount_usd - sum_withdraw_amount_usd) > 0-- cnl2.min_net_deposit_usd::NUMERIC 
--				WHEN um.signup_hostcountry = 'AU' THEN (sum_deposit_amount_usd - sum_withdraw_amount_usd) > cnl2.min_net_deposit_usd::NUMERIC 
--				WHEN um.signup_hostcountry = 'global' THEN (sum_deposit_amount_usd - sum_withdraw_amount_usd) > cnl2.min_net_deposit_usd::NUMERIC 
--				END
		(f.code NOT IN ('TEST','INTERNAL') OR f.code IS NULL)
		AND um.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
--		AND um.user_id = '01EC47XYYXCEVGD9RX5FQSHX6V'
);


CREATE TEMP TABLE IF NOT EXISTS tmp_dm_pcs_user_base_transaction AS
(
	SELECT 
		ub.*
		, SUM(CASE WHEN d.created_at >= NOW()::DATE - '30 day'::INTERVAL THEN d.deposit_count END) l30d_deposit_count
		, SUM(CASE WHEN d.created_at >= NOW()::DATE - '30 day'::INTERVAL THEN d.sum_usd_deposit_amount END) l30d_deposit_usd
		, SUM(CASE WHEN d.created_at >= NOW()::DATE - '30 day'::INTERVAL THEN d.withdraw_count END) l30d_withdraw_count
		, SUM(CASE WHEN d.created_at >= NOW()::DATE - '30 day'::INTERVAL THEN d.sum_usd_withdraw_amount END) l30d_withdraw_usd
		, SUM(CASE WHEN d.created_at >= NOW()::DATE - '30 day'::INTERVAL THEN d.trade_count END) l30d_trade_count
		, SUM(CASE WHEN d.created_at >= NOW()::DATE - '30 day'::INTERVAL THEN d.sum_usd_trade_amount  END) l30d_trade_usd
		, SUM(CASE WHEN d.created_at BETWEEN ub.pcs_tagged_at::DATE AND ub.pcs_tagged_at::DATE + '31 day'::INTERVAL THEN d.sum_usd_trade_amount  END) f1m_trade_usd
		, SUM(CASE WHEN d.created_at >= NOW()::DATE - '30 day'::INTERVAL THEN zd.transfer_to_zwallet_usd END) l30d_transfer_to_zwallet_usd
		, SUM(CASE WHEN d.created_at >= NOW()::DATE - '30 day'::INTERVAL THEN zd.withdraw_from_zwallet_usd END) l30d_withdraw_from_zwallet_usd
	FROM tmp_dm_pcs_user_base ub
		LEFT JOIN reportings_data.dm_user_transactions_dwt_hourly d
			ON ub.user_id = d.user_id 
		LEFT JOIN reportings_data.dm_zw_hourly_transations zd 
			ON ub.ap_account_id = zd.ap_account_id 
			AND zd.product_1_symbol IN ('BTC','SOL','USDC','USDT','XRP','ZMT','ADA','ETH')
	WHERE 
		ub.near_pcs_status != 'non_pcs'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
);


CREATE TEMP TABLE IF NOT EXISTS tmp_dm_pcs_user_wallet AS 
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
				tmp_dm_pcs_user_base dpu 
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
			dpu.near_pcs_status != 'non_pcs'
			AND a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
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
);


CREATE TEMP TABLE IF NOT EXISTS tmp_dm_pcs_individual_report AS
(
	SELECT 
		NOW()::DATE data_as_of
		, ub.*
		, zr.zmt_release_this_month
		, zr.zmt_release_next_month
		, uw.balanced_at
		, uw.asset_on_platform_usd
		, uw.zmt_trade_wallet_amount_usd
		, uw.zmt_z_wallet_amount_usd
		, uw.zmt_zipup_usd
		, uw.non_zmt_trade_wallet_amount_usd
		, uw.non_zmt_z_wallet_amount_usd
		, uw.non_zmt_zipup_usd
	FROM tmp_dm_pcs_user_base_transaction ub
		LEFT JOIN 
			tmp_dm_pcs_user_wallet uw 
			ON ub.ap_account_id = uw.ap_account_id
		LEFT JOIN 
			tmp_ziplock_release zr 
			ON ub.user_id = zr.user_id
	WHERE 
		ub.near_pcs_status != 'non_pcs'
);

INSERT INTO warehouse.bo_testing.dm_pcs_user
(SELECT * FROM tmp_dm_pcs_individual_report);

DROP TABLE IF EXISTS tmp_ziplock_release;
DROP TABLE IF EXISTS tmp_dm_pcs_user_base;
DROP TABLE IF EXISTS tmp_dm_pcs_user_base_transaction;
DROP TABLE IF EXISTS tmp_dm_pcs_user_wallet;
DROP TABLE IF EXISTS tmp_dm_pcs_individual_report;