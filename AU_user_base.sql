-- v.1
WITH base AS (
SELECT 
	a.created_at 
	, u.signup_hostcountry 
	, a.ap_account_id 
	, CASE WHEN a.ap_account_id IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029)
			THEN TRUE ELSE FALSE END AS is_nominee 
	, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
	, a.symbol 
	, u.zipup_subscribed_at 
	, u.is_zipup_subscribed 
	, trade_wallet_amount
	, z_wallet_amount
	, ziplock_amount
	, COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) usd_rate 
	, CASE WHEN a.symbol = 'USD' THEN trade_wallet_amount * 1
				ELSE trade_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END trade_wallet_amount_usd
	, z_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price) z_wallet_amount_usd
	, ziplock_amount * COALESCE(c.average_high_low, g.mid_price, z.price) ziplock_amount_usd
FROM 
	analytics.wallets_balance_eod a 
	LEFT JOIN 
		analytics.users_master u 
		ON a.ap_account_id = u.ap_account_id 
	LEFT JOIN oms_data_public.cryptocurrency_prices c 
	    ON ((CONCAT(a.symbol, 'USD') = c.instrument_symbol) OR (c.instrument_symbol = 'MIOTAUSD' AND a.symbol ='IOTA'))
	    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
	LEFT JOIN public.daily_closing_gold_prices g 
		ON ((DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)) 
		OR (DATE_TRUNC('day', a.created_at) = '2021-07-31 00:00:00' AND DATE_TRUNC('day', g.created_at) = '2021-07-30 00:00:00'))
		AND a.symbol = 'GOLD'
	LEFT JOIN public.daily_ap_prices z
		ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
		AND ((z.instrument_symbol = 'ZMTUSD' AND a.symbol = 'ZMT')
		OR (z.instrument_symbol = 'C8PUSDT' AND a.symbol = 'C8P'))
	LEFT JOIN oms_data_public.exchange_rates e
		ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
		AND e.product_2_symbol  = a.symbol
		AND e."source" = 'coinmarketcap'
WHERE 
	a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL 
	AND u.signup_hostcountry NOT IN ('test', 'error','xbullion')
--	AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
	AND a.symbol NOT IN ('TST1','TST2')
--	AND a.symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
ORDER BY 1 DESC 
)	, aum_snapshot AS (
SELECT 
	DATE_TRUNC('day', created_at) created_at 
	, signup_hostcountry
	, ap_account_id 
	, symbol 
	, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
	, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
	, SUM( COALESCE (ziplock_amount, 0)) ziplock_amount
	, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
	, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
	, SUM( COALESCE (z_wallet_amount_usd, 0)) ziplock_amount_usd
FROM 
	base 
WHERE 
	is_asset_manager = FALSE AND is_nominee = FALSE 
GROUP BY 
	1,2,3,4
ORDER BY 
	1 DESC 
)
SELECT 
  um.created_at
  , um.user_id 
  , um.ap_user_id
  , um.ap_account_id
  , um.email 
  , um.signup_hostname 
  , u.invitation_code 
  , level_increase_status 
  , is_verified 
  , is_email_verified
  , is_mobile_verified
  , sum_trade_volume_usd 
  , sum_deposit_amount_usd 
  , sum_withdraw_amount_usd
  , t.symbol 
  , t.trade_wallet_amount 
  , t.z_wallet_amount 
  , t.ziplock_amount 
  , t.trade_wallet_amount_usd
  , t.z_wallet_amount_usd
  , t.ziplock_amount_usd
FROM analytics.users_master um 
  LEFT JOIN user_app_public.users u
  ON um.user_id = u.id 
  LEFT JOIN aum_snapshot t 
  ON um.ap_account_id = t.ap_account_id
WHERE 
  um.signup_hostcountry = 'AU'
  AND um.ap_account_id = 135292
ORDER BY 1 DESC 
;

-- v.2
WITH base AS (
	SELECT 
		a.created_at 
		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
		, a.ap_account_id 
		, CASE WHEN a.ap_account_id IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029)
				THEN TRUE ELSE FALSE END AS is_nominee 
		, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
		, a.symbol 
		, trade_wallet_amount
		, z_wallet_amount
		, ziplock_amount
		, r.price usd_rate 
		, CASE 
				WHEN r.product_type = 1 THEN trade_wallet_amount * (1/r.price)
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
		a.created_at >= DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL 
		AND a.created_at < DATE_TRUNC('day', NOW()) 
		AND u.signup_hostcountry IN ('AU','global')
		AND a.symbol NOT IN ('TST1','TST2')
	ORDER BY 1 DESC 
	)	, aum_snapshot AS (
	SELECT 
		DATE_TRUNC('day', created_at) created_at 
		, ap_account_id 
		, symbol 
		, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
		, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
		, SUM( COALESCE (ziplock_amount, 0)) ziplock_amount
		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
	FROM 
		base 
	WHERE is_asset_manager = FALSE AND is_nominee = FALSE 
	GROUP BY 
		1,2,3
	ORDER BY 
		1 DESC 
)
SELECT 
	um.created_at  , um.user_id   , um.ap_user_id  , um.ap_account_id  , up.email , up.dob::DATE dob  , up.mobile_number   , um.signup_hostname , um.first_deposit_at
	, u.invitation_code   , level_increase_status   , is_verified   , is_email_verified
	, is_mobile_verified  , sum_trade_volume_usd   , sum_deposit_amount_usd   , sum_withdraw_amount_usd  , t.symbol 
	, COALESCE(t.trade_wallet_amount, 0) trade_wallet_amount  
	, COALESCE(t.z_wallet_amount, 0) z_wallet_amount  
	, COALESCE(t.ziplock_amount, 0) ziplock_amount  	
	, COALESCE(t.trade_wallet_amount_usd, 0) trade_wallet_amount_usd  
	, COALESCE(t.z_wallet_amount_usd, 0) z_wallet_amount_usd  
	, COALESCE(t.ziplock_amount_usd, 0) ziplock_amount_usd
FROM 
	analytics.users_master um 
	LEFT JOIN
	    analytics_pii.users_pii up 
		ON um.user_id = up.user_id
	LEFT JOIN 
	user_app_public.users u
		ON um.user_id = u.id 
	LEFT JOIN 
	aum_snapshot t 
		ON um.ap_account_id = t.ap_account_id
WHERE 
    um.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
    AND um.signup_hostcountry IN ('AU', 'global')
--	[[AND um.signup_hostcountry = {{signup_hostcountry}}]]
--	[[AND um.email = {{email}}]]
ORDER BY 1
;


-- v3. user base with survey info
WITH survey_info AS (
	SELECT 
		user_id 
		, info ->> 'permanent_address' permanent_address
		, info ->> 'permanent_address_postal_code' permanent_address_postal_code
		, info ->> 'present_address' present_address
		, info ->> 'present_address_postal_code' present_address_postal_code
		, info ->> 'address_in_id_card' address_in_id_card
		, info ->> 'address_in_id_card_postal_code' address_in_id_card_postal_code
	FROM user_app_public.personal_infos pi2 
	WHERE archived_at IS NULL 
)	, frankie_info AS (
	SELECT DISTINCT 
		user_id 
		, frankie_entity_id 
	FROM user_app_public.applicant_data ad 
)	, doc_info AS (
	SELECT
		user_id 
		, first_name 
		, last_name 
		, document_type 
		, document_number 
		, dob 
		, country document_country
		, ROW_NUMBER () OVER (PARTITION BY od.user_id ORDER BY od.inserted_at DESC) row_ 
	FROM user_app_public.onfido_documents od 
	WHERE archived_at IS NULL 
)	, personal_info AS (
SELECT
	u2.id 
	, u2.email 
	, um.is_verified 
	, um.level_increase_status
	, d.first_name 
	, d.last_name 
	, d.document_type 
	, d.document_number 
	, d.dob 
	, d.document_country
	, f.frankie_entity_id
	, fsa.frankieone_smart_ui_submitted_at
	, s.permanent_address
	, s.permanent_address_postal_code
	, s.present_address
	, s.present_address_postal_code
	, s.address_in_id_card
	, s.address_in_id_card_postal_code
	, u2.inserted_at::DATE registered_date 
	, CASE WHEN f.frankie_entity_id IS NULL THEN TRUE ELSE FALSE END AS frankie_null
FROM user_app_public.users u2 
	LEFT JOIN doc_info d
		ON u2.id = d.user_id
		AND row_ = 1
	LEFT JOIN frankie_info f 
		ON u2.id = f.user_id
	LEFT JOIN survey_info s 
		ON u2.id = s.user_id
	LEFT JOIN analytics.users_master um 
		ON u2.id = um.user_id
	LEFT JOIN analytics.frankieone_submitted_at fsa 
	    ON u2.id = fsa.user_id
WHERE 
	um.is_verified = TRUE 
	AND um.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
	AND um.signup_hostcountry IN ('global')
	AND u2.email NOT LIKE '%zipmex.com'
	AND u2.email NOT LIKE '%alphapoint.com'
)
SELECT 
	COUNT(DISTINCT CASE WHEN frankie_entity_id IS NULL THEN id END) distinct_entity_count
	, COUNT(DISTINCT CASE WHEN frankieone_smart_ui_submitted_at IS NULL THEN id END) distinct_smartui_count
	, COUNT(id) all_count
FROM personal_info
;

