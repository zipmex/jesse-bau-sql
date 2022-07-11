---- MTU 2021-09-21
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
		, a.ap_account_id 
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
			(CASE WHEN a.created_at < '2022-05-24' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
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
		LEFT JOIN 
			mappings.users_mapping um 
			ON a.ap_account_id = um.ap_account_id 
	WHERE 
		a.created_at >= '2022-06-01' AND a.created_at < '2022-07-01'
	-- exclude test products
		AND a.symbol NOT IN ('TST1','TST2')
	    AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
		AND zc.symbol IS NOT NULL
	ORDER BY 1 DESC 
)	, aum_snapshot AS (
	SELECT 
		a.created_at 
		, a.signup_hostcountry 
		, a.ap_account_id 
		, CASE WHEN symbol <> 'ZMT' AND zipup_coin = TRUE THEN 'zipup_coin' 
				WHEN symbol = 'ZMT' THEN 'ZMT' 
				ELSE 'non_zipup' END AS asset_type
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		, SUM( COALESCE (zlaunch_amount_usd, 0)) zlaunch_amount_usd
		, SUM( COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND a.created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND zipup_coin = TRUE
					THEN
						(CASE 	WHEN a.created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
								WHEN a.created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
					END, 0)) AS zipup_subscribed_usd
	FROM 
		base a 
	WHERE  
		signup_hostcountry IS NOT NULL 
		AND signup_hostcountry <> 'test'
		AND is_nominee = FALSE AND is_asset_manager = FALSE
	GROUP BY 
		1,2,3,4
	ORDER BY 
		1 DESC
)	, active_zipup_balance AS (
	SELECT 
		created_at 
		, signup_hostcountry
		, ap_account_id 
		, SUM( CASE WHEN asset_type = 'ZMT' THEN zipup_subscribed_usd END) zmt_zw_usd_amount
		, SUM( CASE WHEN asset_type <> 'ZMT' THEN zipup_subscribed_usd END) nonzmt_zw_usd_amount
	FROM 
		aum_snapshot a 
	WHERE 
		asset_type <> 'non_zipup'
	GROUP BY 1,2,3
)	, active_ziplock_balance AS (
	SELECT 
		created_at 
		, signup_hostcountry
		, ap_account_id 
		, SUM( CASE WHEN asset_type = 'ZMT' THEN ziplock_amount_usd END) zmt_lock_usd_amount
		, SUM( CASE WHEN asset_type <> 'ZMT' THEN ziplock_amount_usd END) nonzmt_lock_usd_amount
	FROM 
		aum_snapshot a 
	WHERE 
		asset_type <> 'non_zipup'
	GROUP BY 1,2,3
)	, active_user AS (
SELECT 
	COALESCE (DATE_TRUNC('month', u.created_at), DATE_TRUNC('month', l.created_at)) created_at 
	, COALESCE (u.signup_hostcountry, l.signup_hostcountry) signup_hostcountry
	, COALESCE (u.ap_account_id, l.ap_account_id) ap_account_id
	, CASE WHEN nonzmt_zw_usd_amount >= 1 THEN u.ap_account_id END AS zipup_zw_user
	, CASE WHEN (COALESCE (zmt_lock_usd_amount,0) + COALESCE (nonzmt_lock_usd_amount,0)) >= 1 THEN l.ap_account_id 
			END AS total_ziplock_user
	, CASE WHEN COALESCE (zmt_lock_usd_amount,0) >= 1 AND COALESCE (nonzmt_lock_usd_amount,0) >= 1 THEN l.ap_account_id 
			END AS ziplock_mix_user
	, CASE WHEN COALESCE (zmt_lock_usd_amount,0) < 1 AND COALESCE (nonzmt_lock_usd_amount,0) >= 1 THEN l.ap_account_id 
			END AS ziplock_nozmt_user
	, CASE WHEN COALESCE (zmt_lock_usd_amount,0) >= 1 AND COALESCE (nonzmt_lock_usd_amount,0) < 1 THEN l.ap_account_id 
			END AS ziplock_zmt_user
	, CASE WHEN (COALESCE (zmt_zw_usd_amount,0) >= 1 OR COALESCE (nonzmt_zw_usd_amount,0) >= 1 
				OR COALESCE (zmt_lock_usd_amount,0) >= 1 OR COALESCE (nonzmt_lock_usd_amount,0) >= 1) 
			THEN u.ap_account_id END AS active_balance_user
FROM 
	active_zipup_balance u 
	FULL OUTER JOIN active_ziplock_balance l 
		ON u.created_at = l.created_at
		AND u.ap_account_id = l.ap_account_id
		AND u.signup_hostcountry = l.signup_hostcountry
WHERE
	(zmt_zw_usd_amount >= 1 OR zmt_lock_usd_amount >= 1 OR nonzmt_lock_usd_amount >= 1 OR nonzmt_zw_usd_amount >= 1)
)
SELECT 
	created_at 
	, signup_hostcountry
--	, COUNT( DISTINCT total_ziplock_user) total_ziplock_user
--	, COUNT( DISTINCT ziplock_mix_user) ziplock_mix_user
--	, COUNT( DISTINCT ziplock_nozmt_user) ziplock_nozmt_user
--	, COUNT( DISTINCT ziplock_zmt_user) ziplock_zmt_user
	, COUNT( DISTINCT active_balance_user) active_balance_count
FROM active_user
GROUP BY 1,2


