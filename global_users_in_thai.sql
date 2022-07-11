DROP TABLE IF EXISTS tmp_gl_user_reside_th;

CREATE TEMP TABLE IF NOT EXISTS tmp_gl_user_reside_th AS (
-- global users using NRIC
SELECT 
	od.user_id 
	, up.email 
	, um.ap_account_id
	, od.country document_country
	, um.signup_hostcountry
	, pi2.info ->> 'present_address_country' pi_present_address_country
	, COUNT(DISTINCT od.user_id) user_count
FROM user_app_public.onfido_documents od 
	LEFT JOIN 
		analytics_pii.users_pii up  
		ON od.user_id = up.user_id  
	LEFT JOIN 
		analytics.users_master um 
		ON od.user_id = um.user_id 
	LEFT JOIN 
		user_app_public.personal_infos pi2 
		ON od.user_id = pi2.user_id 
		AND pi2.archived_at IS NULL
WHERE 
	od.archived_at IS NULL 
	AND um.signup_hostcountry = 'global'
	AND um.is_verified = TRUE
	AND (od.country = 'THA' OR pi2.info ->> 'present_address_country' = 'THA')
	AND up.email NOT LIKE '%@zipmex%' 
--	AND od.document_type IN ('national_identity_card','OTHER','passport')
GROUP BY 1,2,3,4,5,6
);


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
		, th.ap_account_id , th.email , th.document_country, th.pi_present_address_country , dmm.mtu
	-- filter nominee accounts from users_mapping
		, CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121 ,496001))
			THEN TRUE ELSE FALSE END AS is_nominee 
	-- filter asset_manager account
		, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
	-- zipup subscribe status to identify zipup amount
		, u.zipup_subscribed_at
		, CASE WHEN ult.tier_name IS NULL THEN 'no_zmt' ELSE ult.tier_name END AS vip_tier
		, a.symbol
		, CASE WHEN a.symbol = 'ZMT' THEN TRUE 
				WHEN zc.symbol IS NOT NULL THEN TRUE 
				ELSE FALSE END AS zipup_coin 
		, r.price usd_rate , r.product_type 
		, trade_wallet_amount
		, CASE	WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
				WHEN r.product_type = 2 THEN trade_wallet_amount * r.price
				END AS trade_wallet_amount_usd
		, z_wallet_amount * r.price z_wallet_amount_usd
		, ziplock_amount * r.price ziplock_amount_usd
		, zlaunch_amount * r.price zlaunch_amount_usd
	FROM 
		analytics.wallets_balance_eod a 
		RIGHT JOIN tmp_gl_user_reside_th th 
			ON a.ap_account_id = th.ap_account_id
	-- get country and join with pii data
		LEFT JOIN 
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
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
			zip_lock_service_public.user_loyalty_tiers ult 
			ON u.user_id = ult.user_id 
		LEFT JOIN 
			analytics.dm_mtu_monthly dmm 
			ON th.ap_account_id = dmm.ap_account_id 
			AND dmm.mtu_month = '2022-06-01'
	WHERE 
		a.created_at >= '2022-07-01' --AND a.created_at < '2022-07-01'
	-- exclude test products
		AND a.symbol NOT IN ('TST1','TST2')
	    AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
	ORDER BY 1 DESC 
)
	SELECT 
		DATE_TRUNC('day', b.created_at)::DATE created_at
		, email, document_country, pi_present_address_country , mtu mtu_june
--		, CASE WHEN symbol <> 'ZMT' AND zipup_coin = TRUE THEN 'zipup_coin' 
--				WHEN symbol = 'ZMT' THEN 'ZMT' 
--				ELSE 'other' END AS asset_group
--		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
--		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
--		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
--		, SUM( COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND b.created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND zipup_coin = TRUE
--					THEN
--						(CASE 	WHEN b.created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
--								WHEN b.created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
--					END, 0)) AS zipup_subscribed_usd
		, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) 
					+ COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0)) total_aum_usd
	FROM 
		base b
	GROUP BY 
		1,2,3,4,5
	ORDER BY 1,2
;