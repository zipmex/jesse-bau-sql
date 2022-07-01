SELECT 
	DATE_TRUNC('year', um.created_at)::DATE created_at 
	, um.signup_hostcountry 
--	, um.signup_platform 
	, CASE WHEN circ.referral_group IS NOT NULL THEN circ.referral_group 
			WHEN grc.referral_group IS NOT NULL THEN grc.referral_group 
			WHEN um.invitation_code IS NULL THEN 'organic'
			ELSE 'p2p'
			END AS referral_group
	, COUNT(DISTINCT um.user_id) user_count
	, COUNT(DISTINCT CASE WHEN is_verified = TRUE THEN um.user_id END) verified_user_count
FROM analytics.users_master um 
	LEFT JOIN mappings.commercial_indo_referral_code circ 
		ON LOWER(um.invitation_code) = LOWER(circ.referral_code)
	LEFT JOIN mappings.growth_referral_code grc 
		ON LOWER(um.invitation_code) = LOWER(grc.referral_code)
WHERE 
	um.signup_hostcountry IN ('TH', 'ID', 'AU', 'global')
GROUP BY 1,2,3
ORDER BY 1 DESC 
;



SELECT 
	tm.created_at::DATE
	, tm.product_1_symbol 
	, COALESCE (SUM(CASE WHEN side = 'Buy' THEN tm.amount_usd END), 0) AS buy_amount_usd
	, COALESCE (SUM(CASE WHEN side = 'Sell' THEN tm.amount_usd END), 0) AS sell_amount_usd
	, COALESCE (SUM(CASE WHEN tm.side = 'Buy' THEN fm.fee_usd_amount END), 0) AS fee_buy_usd
	, COALESCE (SUM(CASE WHEN tm.side = 'Sell' THEN fm.fee_usd_amount END), 0) AS fee_sell_usd
FROM 
	analytics.trades_master tm 
	LEFT JOIN analytics.fees_master fm 
		ON tm.execution_id = fm.fee_reference_id 
WHERE 
	tm.signup_hostcountry IN ('TH', 'ID', 'AU', 'global')
;


-- last-lock-campaign
WITH base AS (
	SELECT 
		(lt.locked_at + '7 hour'::INTERVAL)::DATE lock_time_gmt7
		, (lt.locked_at + '11 hour'::INTERVAL)::DATE lock_time_gmt11
		, lt.locked_at::DATE lock_time_utc
		, lt.user_id 
		, upper(SPLIT_PART(lt.product_id,'.',1)) symbol
		, SUM(lt.amount) lock_amount_unit
	FROM 
-- check new ziplock time
		zip_lock_service_public.lock_transactions lt
	WHERE
-- filter succeed transactions 
		lt.status = 'completed'
-- campaign period >= 2022-03-09
		AND lt.locked_at>= '2022-03-09'
	GROUP BY 1,2,3,4,5
)	, lock_transaction AS (
	SELECT 
--		lt.lock_time_gmt7
		lt.lock_time_gmt11
		, lt.lock_time_utc
-- new users: being referred after 2022-03-08
		, (um.created_at + '11 hours'::INTERVAL)::DATE register_gmt11
		, um.ap_account_id 
		, up.email 
		, um.invitation_code 
		, um.signup_hostcountry 
		, lt.symbol
		, SUM(lt.lock_amount_unit) lock_amount_unit
		, SUM(lt.lock_amount_unit * rm.price) lock_amount_usd
	FROM base lt  
		LEFT JOIN analytics.users_master um 
			ON lt.user_id = um.user_id 
	-- get pii email
		LEFT JOIN analytics_pii.users_pii up 
			ON lt.user_id = up.user_id 
	-- conver lock amount to lock amount usd 
		LEFT JOIN analytics.rates_master rm 
			ON lt.symbol = rm.product_1_symbol 
			AND lock_time_utc = rm.created_at::DATE 
	WHERE
		um.signup_hostcountry IN ('TH','AU','ID')
		AND um.ap_account_id NOT IN (SELECT DISTINCT ap_account_id::NUMERIC FROM mappings.users_mapping um)
	GROUP BY 1,2,3,4,5,6,7,8
)	, lock_convert AS (
	SELECT 
		lt.*
--		, SUM( CASE WHEN cp.product_2_symbol = 'THB' OR ap.product_2_symbol = 'THB'
--				THEN lt.lock_amount_unit * COALESCE(cp.average_high_low, ap.price) END) lock_amount_thb
		, SUM( CASE WHEN cp.product_2_symbol = 'AUD' OR ap.product_2_symbol = 'AUD'
				THEN lt.lock_amount_unit * COALESCE(cp.average_high_low, ap.price) END) lock_amount_aud
--		, SUM( CASE WHEN cp.product_2_symbol = 'IDR' OR ap.product_2_symbol = 'IDR'
--				THEN lt.lock_amount_unit * COALESCE(cp.average_high_low, ap.price) END) lock_amount_idr
	FROM lock_transaction lt
-- convert lock amount to lock_amount_AUD - for non ZMT token
		LEFT JOIN mappings.public_cryptocurrency_prices cp 
			ON lt.symbol = cp.product_1_symbol 
			AND lock_time_utc = cp.last_updated::DATE
-- convert lock amount to lock_amount_AUD - for ZMT token
		LEFT JOIN mappings.public_daily_ap_prices ap 
			ON lt.symbol = ap.product_1_symbol 
			AND lock_time_utc = ap.created_at::DATE
	GROUP BY 1,2,3,4,5,6,7,8,9,10
	ORDER BY 4
)	, final_th_result AS (
	SELECT 
--		lc.lock_time_gmt11
		lc.register_gmt11
		, lc.ap_account_id
		, lc.email
		, lc.invitation_code
		, lc.signup_hostcountry
		, SUM(lc.lock_amount_usd) lock_amount_usd
		, SUM(lc.lock_amount_aud) lock_amount_aud
--		, SUM(lock_amount_thb) OVER(PARTITION BY lc.ap_account_id) total_lock_amount_thb
--		, SUM( CASE WHEN lc.symbol NOT IN ('ZMT') THEN lock_amount_thb END) OVER(PARTITION BY lc.ap_account_id) non_ZMT_lock_amount_thb
--		, SUM(lock_amount_aud) OVER(PARTITION BY lc.ap_account_id) total_lock_amount_aud
--		, SUM(lock_amount_idr) OVER(PARTITION BY lc.ap_account_id) total_lock_amount_idr
	FROM lock_convert lc
	GROUP BY 1,2,3,4,5
)	, referral AS (
	SELECT 
		lc.*
		, um2.ap_account_id referrer_account_id
		, up2.email referrer_email
	FROM 
		final_th_result lc 
-- get referree of new users
		LEFT JOIN 
			analytics.users_master um2
			ON lc.invitation_code = um2.referral_code 
-- get email of the referree
		LEFT JOIN 
			analytics_pii.users_pii up2 
			ON um2.user_id = up2.user_id 
)
SELECT 
	r.*
	, ft.lock_amount_aud referrer_total_lock_aud
-- eligible users: refer new users after 2022-03-08, new user lock >= 100 AUD, referree lock >= 100 AUD
	, CASE WHEN r.register_gmt11 >= '2022-03-09' AND r.lock_amount_aud >= 100 AND ft.lock_amount_aud >= 100 THEN TRUE ELSE FALSE END AS is_eligible
	, pap.product_1_symbol airdrop_symbol
	, pap.product_2_symbol airdrop_fiat_conversion
-- new users, if eligible, get 30 AUD in ZMT
	, CASE WHEN r.register_gmt11 >= '2022-03-09' AND r.lock_amount_aud >= 100 AND ft.lock_amount_aud >= 100 THEN ROUND( 30.0 * 1/pap.price::NUMERIC, 8) END AS zmt_amount_30aud
	, CASE WHEN r.register_gmt11 >= '2022-03-09' AND r.lock_amount_aud >= 100 AND ft.lock_amount_aud >= 100 THEN r.email END AS airdrop_30aud
-- referree, if eligible, get 70 AUD in ZMT
	, CASE WHEN r.register_gmt11 >= '2022-03-09' AND r.lock_amount_aud >= 100 AND ft.lock_amount_aud >= 100 THEN ROUND( 70.0 * 1/pap.price::NUMERIC, 8) END AS zmt_amount_70aud
	, CASE WHEN r.register_gmt11 >= '2022-03-09' AND r.lock_amount_aud >= 100 AND ft.lock_amount_aud >= 100 THEN r.referrer_email END AS airdrop_70aud
FROM referral r
-- get lock amount AUD of the referree 
	LEFT JOIN final_th_result ft 
		ON r.referrer_account_id = ft.ap_account_id
-- convert airdrop amount from AUD to ZMT using the latest conversion rate
	CROSS JOIN 
		(SELECT *
		FROM mappings.public_ap_prices pap 
		WHERE 
			pap.product_1_symbol = 'ZMT'
			AND pap.product_2_symbol = 'AUD'
		ORDER BY inserted_at DESC 
		LIMIT 1) pap
WHERE r.signup_hostcountry = 'AU'
ORDER BY 2 DESC
;

