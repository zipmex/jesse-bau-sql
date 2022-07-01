-- user persona 2021 first draft 
SELECT 
	sd.ap_account_id 
	, age_ 
	, income 
	, persona 
	, um.created_at::date,
	um.age,
	um.signup_hostcountry,
	um.signup_platform,
	um.level_increase_status,
	um.onfido_completed_at::date,
	um.onfido_submitted_at::date,
	um.traffic_agency,
	um.traffic_channel_group,
	um.traffic_medium_group,
	um.traffic_source_group,
	um.traffic_campaign_group,
	um.has_deposited,
	um.first_deposit_at::date,
	um.has_traded,
	um.first_traded_at::date,
	um.is_zipup_subscribed,
	um.zipup_subscribed_at::date,
	ba.review_status,
	ss.inserted_at::date,
	p.info::jsonb ->> 'dob' as dob,
	ss.survey::jsonb ->> 'gender' as gender,
	p.info::jsonb ->> 'present_address_province' as present_address_province,
	p.info::jsonb ->> 'nationality' as nationality,
	ss.survey::jsonb ->> 'education' as education,
	p.info::jsonb ->> 'company_name' as company_name,
	p.info::jsonb ->> 'occupation' as occupation,
	p.info::jsonb ->> 'work_position' as work_position,
	ss.survey::jsonb ->> 'total_estimate_monthly_income' as Estimate_Monthly_Income,
	ss.survey::jsonb ->> 'expenses' as expenses,
	ss.survey::jsonb ->> 'fin_status' as fin_status,
	ss.survey::jsonb ->> 'objective' as objective,
	ss.survey::jsonb ->> 'investment_xp' as investment_xp,
	ss.survey::jsonb ->> 'understand_digital_assets' as understand_digital_assets
FROM 
	bo_testing.sample_demo_20211118 sd 
	LEFT JOIN analytics.users_master um 
		ON sd.ap_account_id = um.ap_account_id 
	LEFT JOIN user_app_public.personal_infos p
		ON um.user_id = p.user_id 
		AND p.archived_at IS NULL 
	LEFT JOIN user_app_public.suitability_surveys ss 
		ON um.user_id = ss.user_id 
		AND ss.archived_at IS NULL
	LEFT JOIN user_app_public.bank_accounts ba 
		ON um.user_id = ba.user_id 
WHERE um.signup_hostcountry = 'TH'
ORDER BY 1
;

-- pcs th AUM monthly
WITH pcs_th AS (
-- pcs TH conditions
	SELECT 
		zte.created_at::DATE vip_date
		, zte.signup_hostcountry 
		, zte.ap_account_id 
		, um.created_at::DATE register_date
	-- vip4 by end of month
		, CASE WHEN vip_tier = 'vip4' THEN TRUE ELSE FALSE END AS is_pcs
		, vip_tier 
	FROM 
	-- vip record by end of month 
		analytics.zmt_tier_endofmonth zte  
		LEFT JOIN analytics.users_master um
			ON zte.ap_account_id = um.ap_account_id 
	WHERE 
		zte.signup_hostcountry = 'TH'
		AND zte.created_at = DATE_TRUNC('month', zte.created_at) + '1 month - 1 day'::INTERVAL
		AND zte.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping um)
)	, aum_base AS (
	SELECT 
		a.created_at 
		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
		, a.ap_account_id , pc.is_pcs, pc.vip_tier , pc.vip_date , pc.register_date
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
			analytics_pii.users_pii up 
			ON u.user_id = up.user_id 
		RIGHT JOIN
			pcs_th pc 
			ON a.ap_account_id = pc.ap_account_id
			AND a.created_at::DATE = pc.vip_date
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE 
		a.created_at >= '2022-01-01' AND a.created_at < NOW()::DATE
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
		AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND a.symbol NOT IN ('TST1','TST2')
	ORDER BY 1 DESC 
)--	, aum_snapshot AS (
SELECT 
	DATE_TRUNC('month', created_at)::DATE created_at
	, register_date , vip_date
	, signup_hostcountry
	, ap_account_id
	, is_pcs
	, vip_tier 
--	, CASE WHEN symbol IN ('BTC','ETH','GOLD','LTC','USDC','USDT') THEN 'zipup_coin' 
--			WHEN symbol = 'ZMT' THEN 'ZMT' 
--			ELSE 'other' END AS asset_group
	, SUM( COALESCE ( CASE WHEN symbol = 'ZMT' THEN ziplock_amount END, 0)) zmt_lock_amount
	, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
	, SUM( COALESCE (CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
				THEN
					(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
							WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
				END, 0)) AS zwallet_subscribed_usd
	, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0)) total_aum_usd
FROM 
	aum_base 
GROUP BY 
	1,2,3,4,5,6,7
ORDER BY 
	1 
;


-- pcs th trade monthly
WITH pcs_th AS (
-- pcs TH conditions
	SELECT 
		zte.created_at::DATE vip_date
		, zte.signup_hostcountry 
		, zte.ap_account_id 
		, um.created_at::DATE register_date
	-- vip4 by end of month
		, CASE WHEN vip_tier = 'vip4' THEN TRUE ELSE FALSE END AS is_pcs
		, vip_tier 
	FROM 
	-- vip record by end of month 
		analytics.zmt_tier_endofmonth zte  
		LEFT JOIN analytics.users_master um
			ON zte.ap_account_id = um.ap_account_id 
	WHERE 
		zte.signup_hostcountry = 'TH'
		AND zte.created_at = DATE_TRUNC('month', zte.created_at) + '1 month - 1 day'::INTERVAL
		AND zte.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping um)
)
SELECT 
	DATE_TRUNC('month', tm.created_at)::DATE
	, tm.ap_account_id 
	, register_date
	, vip_date
	, is_pcs
	, vip_tier 
	, SUM(tm.amount_usd) trade_volume_usd
	, COUNT(DISTINCT ap_account_id) pcs_count
FROM 
	analytics.trades_master tm 
	RIGHT JOIN 
		pcs_th p 
		ON tm.ap_account_id = p.ap_account_id
		AND DATE_TRUNC('month', tm.created_at)::DATE = DATE_TRUNC('month', p.vip_date)
WHERE 
	tm.created_at::DATE >= '2022-01-01'
GROUP BY 1,2,3,4,5,6
;


-- pcs th list - avg aum
WITH pcs_user AS (
	SELECT 
		zte.created_at::DATE vip_date
		, zte.signup_hostcountry 
		, zte.ap_account_id 
		, um.created_at::DATE register_date
	-- vip4 by end of month
		, CASE WHEN vip_tier = 'vip4' THEN TRUE ELSE FALSE END AS is_pcs
		, vip_tier 
		, CASE WHEN DATE_TRUNC('month', zte.created_at)::DATE = DATE_TRUNC('month', um.created_at)::DATE THEN 'new_user' ELSE 'current_user' END AS is_new_user
		, COUNT(DISTINCT zte.ap_account_id) user_count
	FROM 
	-- vip record by end of month 
		analytics.zmt_tier_1stofmonth zte  
		LEFT JOIN analytics.users_master um
			ON zte.ap_account_id = um.ap_account_id 
	WHERE 
		zte.signup_hostcountry = 'TH'
		AND zte.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping um)
		AND zte.vip_tier = 'vip4'
	GROUP BY 1,2,3,4,5,6,7
)	, base AS (
	SELECT 
		a.created_at 
		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
		, pu.ap_account_id , up.email , u.user_id 
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
	-- get country and join with pii data
		RIGHT JOIN pcs_user pu 
			ON a.ap_account_id = pu.ap_account_id
			AND DATE_TRUNC('month', a.created_at)::DATE = pu.vip_date::DATE
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
		a.created_at >= '2021-01-01' AND a.created_at < DATE_TRUNC('year', NOW())::DATE
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
	-- snapshot by end of month or yesterday
--		AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
	-- exclude test products
		AND a.symbol NOT IN ('TST1','TST2')
	ORDER BY 1 DESC 
)
SELECT 
	DATE_TRUNC('month', created_at)::DATE created_at
	, signup_hostcountry
	, CASE 
			WHEN symbol = 'ZMT' THEN 'ZMT' 
			ELSE 'other' END AS asset_group
	, COUNT( DISTINCT ap_account_id) user_count
	, SUM( COALESCE (ziplock_amount_usd, 0)) +  SUM( COALESCE (CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
				THEN
					(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
							WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
				END, 0)) + SUM( COALESCE (zlaunch_amount_usd, 0)) AS interest_aum_usd
	, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) 
				+ COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0)) total_aum_usd
	, (SUM( COALESCE (ziplock_amount_usd, 0)) +  SUM( COALESCE (CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
				THEN
					(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
							WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
				END, 0)) + SUM( COALESCE (zlaunch_amount_usd, 0))) / COUNT( DISTINCT ap_account_id) AS avg_interest_aum_usd
	, (SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) 
				+ COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0))) / COUNT( DISTINCT ap_account_id) avg_total_aum_usd
FROM 
	base 
WHERE 
	is_asset_manager = FALSE AND is_nominee = FALSE
GROUP BY 
	1,2,3
ORDER BY 
	1 
;

-- pcs th list - avg trade
WITH pcs_user AS (
	SELECT 
		zte.created_at::DATE vip_date
		, zte.signup_hostcountry 
		, zte.ap_account_id 
		, um.created_at::DATE register_date
	-- vip4 by end of month
		, CASE WHEN vip_tier = 'vip4' THEN TRUE ELSE FALSE END AS is_pcs
		, vip_tier 
		, CASE WHEN DATE_TRUNC('month', zte.created_at)::DATE = DATE_TRUNC('month', um.created_at)::DATE THEN 'new_user' ELSE 'current_user' END AS is_new_user
		, COUNT(DISTINCT zte.ap_account_id) user_count
	FROM 
	-- vip record by end of month 
		analytics.zmt_tier_1stofmonth zte  
		LEFT JOIN analytics.users_master um
			ON zte.ap_account_id = um.ap_account_id 
	WHERE 
		zte.signup_hostcountry = 'TH'
		AND zte.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping um)
		AND zte.vip_tier = 'vip4'
	GROUP BY 1,2,3,4,5,6,7
)
SELECT 
	DATE_TRUNC('month', tm.created_at)::DATE traded_month
	, pu.signup_hostcountry
	, CASE WHEN tm.product_1_symbol = 'ZMT' THEN TRUE ELSE FALSE END AS is_zmt
	, COUNT(DISTINCT pu.ap_account_id) trader_count
	, SUM(tm.amount_usd) sum_trade_volume_usd
	, SUM(tm.amount_usd) / COUNT(DISTINCT pu.ap_account_id) avg_trade_volume_usd
FROM 
	pcs_user pu
	LEFT JOIN analytics.trades_master tm 
		ON pu.ap_account_id = tm.ap_account_id 
		AND pu.vip_date = DATE_TRUNC('month', tm.created_at)::DATE
WHERE 
	tm.created_at >= '2021-01-01' 
	AND tm.created_at < DATE_TRUNC('year', NOW())
GROUP BY 1,2,3
;


-- pcs th list - avg deposit / withdraw
WITH pcs_user AS (
	SELECT 
		zte.created_at::DATE vip_date
		, zte.signup_hostcountry 
		, zte.ap_account_id 
		, um.created_at::DATE register_date
	-- vip4 by end of month
		, CASE WHEN vip_tier = 'vip4' THEN TRUE ELSE FALSE END AS is_pcs
		, vip_tier 
		, CASE WHEN DATE_TRUNC('month', zte.created_at)::DATE = DATE_TRUNC('month', um.created_at)::DATE THEN 'new_user' ELSE 'current_user' END AS is_new_user
		, COUNT(DISTINCT zte.ap_account_id) user_count
	FROM 
	-- vip record by end of month 
		analytics.zmt_tier_1stofmonth zte  
		LEFT JOIN analytics.users_master um
			ON zte.ap_account_id = um.ap_account_id 
	WHERE 
		zte.signup_hostcountry = 'TH'
		AND zte.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping um)
		AND zte.vip_tier = 'vip4'
	GROUP BY 1,2,3,4,5,6,7
)	, base_deposit AS (
	SELECT 
		DATE_TRUNC('month', d.created_at) created_at 
		, pu.ap_account_id 
		, d.signup_hostcountry 
		, SUM(d.amount_usd) deposit_usd
	FROM 
		analytics.withdraw_tickets_master d 
		RIGHT JOIN pcs_user pu 
			ON d.ap_account_id = pu.ap_account_id
			AND DATE_TRUNC('month', d.created_at)::DATE = pu.vip_date
	WHERE 
		d.status = 'FullyProcessed' 
		AND d.signup_hostcountry IN ('TH','AU','ID','global')
		AND DATE_TRUNC('day', d.created_at) >= '2021-01-01'
		AND DATE_TRUNC('day', d.created_at) < '2022-01-01'
		AND d.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) 
	GROUP  BY 
		1,2,3
)
SELECT 
	created_at::DATE 
	, signup_hostcountry 
	, COUNT(DISTINCT ap_account_id) depositor_count
	, SUM(deposit_usd) deposit_usd
	, SUM(deposit_usd) / COUNT(DISTINCT ap_account_id) avg_deposit_usd
FROM base_deposit bd
GROUP BY 1,2
;