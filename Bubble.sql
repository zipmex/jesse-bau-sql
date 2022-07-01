------- user persona
WITH base AS (
SELECT 
	a.created_at 
	, CASE WHEN u.signup_hostcountry IN ('test', 'error','xbullion') THEN 'test' ELSE u.signup_hostcountry END AS signup_hostcountry 
	, a.ap_account_id 
	, CASE WHEN a.ap_account_id IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001) 
			THEN TRUE ELSE FALSE END AS is_nominee
	, a.symbol 
	, u.zipup_subscribed_at 
	, u.is_zipup_subscribed 
	, SUM(trade_wallet_amount) trade_wallet_amount
	, SUM(z_wallet_amount) z_wallet_amount
	, SUM(ziplock_amount) ziplock_amount
	, SUM( CASE WHEN a.symbol = 'USD' THEN trade_wallet_amount * 1
				ELSE trade_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END) trade_wallet_amount_usd
	, SUM( z_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price) ) z_wallet_amount_usd
	, SUM( ziplock_amount * COALESCE(c.average_high_low, g.mid_price, z.price) ) ziplock_amount_usd
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
		OR (z.instrument_symbol = 'C8PUSDT' AND a.symbol = 'C8P')
		OR (z.instrument_symbol = 'TOKUSD' AND a.symbol = 'TOK'))
	LEFT JOIN oms_data_public.exchange_rates e
		ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
		AND e.product_2_symbol  = a.symbol
		AND e."source" = 'coinmarketcap'
WHERE 
	a.created_at = DATE_TRUNC('month', DATE_TRUNC('day', NOW() - '1 day'::INTERVAL)) 
	AND a.symbol NOT IN ('TST1','TST2')
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1 DESC 
)	, aum_snapshot AS (
SELECT 
	DATE_TRUNC('month', a.created_at) created_at 
	, a.signup_hostcountry
	, is_nominee
	, a.ap_account_id 
--	, symbol 
--	, CASE WHEN is_zipup_subscribed = TRUE AND a.created_at >= DATE_TRUNC('day', zipup_subscribed_at) THEN TRUE ELSE FALSE END AS is_zipup
	, CASE WHEN symbol = 'ZMT' THEN 'ZMT' ELSE 'non_zmt' END AS asset_type
	, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
	, SUM( COALESCE (trade_wallet_amount_usd,0)) trade_wallet_amount_usd
	, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
	, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
	, SUM( COALESCE (trade_wallet_amount, 0) + COALESCE (z_wallet_amount, 0)) total_wallet_amount 
	, SUM( COALESCE (trade_wallet_amount_usd,0) + COALESCE (z_wallet_amount_usd, 0)) total_wallet_usd
	, SUM( COALESCE (ziplock_amount, 0)) ziplock_amount
	, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
FROM 
	base a 
WHERE 
	signup_hostcountry IS NOT NULL
	AND is_nominee = FALSE
GROUP BY 
	1,2,3,4,5
ORDER BY 
	1 DESC 
)
SELECT 
	DISTINCT u.user_id	, u.ap_user_id	, u.ap_account_id	
	, u.created_at	, u.email	, u.dob	, u.age	, u.gender	
	, u.mobile_number	, u.referring_user_id	
	, u.signup_hostname	, u.signup_hostcountry	, u.signup_platform	
	, u.is_verified	, u.verification_level	, u.level_increase_status	, u.is_email_verified	, u.is_mobile_verified	, u.is_onfido_verified	
	, u.document_country	, u.document_type	, u.user_segment	
	, u.is_zipup_subscribed	, u.base_fiat	, u.has_traded	, u.first_traded_at	, u.last_traded_at	, u.count_trades	
	, u.sum_trade_volume_usd	, u.count_deposits	, u.sum_deposit_amount_usd	, u.count_withdraws	, u.sum_withdraw_amount_usd	, u.sum_fee_amount_usd
	, COALESCE(p.info ->> 'permanent_address_province',p.info ->> 'address_in_id_card_province',p.info ->> 'present_address_province',p.info ->> 'work_address_province','0') AS reg_province	
	, p.info ->> 'occupation' occupation_info
	, r.number_of_referral 
	, (aa.total_wallet_usd + aa.ziplock_amount_usd) AS balance_nozmtstaked 
	, a.ziplock_amount_usd AS zmt_stake_usd 
	, CASE WHEN a.ziplock_amount >= 20000 THEN 'ZipCrew'
			WHEN a.ziplock_amount >= 100 AND a.ziplock_amount < 20000 THEN 'ZipMember'
			ELSE 'ZipStarter' END AS loyalty_tier 
	, s.survey ->> 'work_position' work_position	, s.survey ->> 'expenses' expenses	, s.survey ->> 'education' education	, s.survey ->> 'objective' objective	, s.survey ->> 'fin_status' fin_status	, s.survey ->> 'is_student' is_student	, s.survey ->> 'nationality' nationality	
	, TRIM(BOTH '[...]' FROM s.survey ->> 'investment_xp') investment_xp , s.survey ->> 'present_address' present_address	, s.survey ->> 'investment_period' investment_period	
	, TRIM(BOTH '"..."' FROM TRIM(BOTH '[...]' FROM s.survey ->> 'primary_source_of_funds')) primary_source_of_funds	, s.survey ->> 'digital_assets_experience' digital_assets_experience	, s.survey ->> 'understand_digital_assets' understand_digital_assets	, s.survey ->> 'total_estimate_monthly_income' total_estimate_monthly_income
FROM analytics.users_master u
	LEFT JOIN user_app_public.personal_infos p 
		ON u.user_id = p.user_id 
		AND p.archived_at IS NULL 
	LEFT JOIN user_app_public.suitability_surveys s 
		ON u.user_id = s.user_id 
		AND s.archived_at IS NULL 
	LEFT JOIN 
		(
			SELECT 
				um.ap_account_id 
				, COUNT( CASE WHEN um2.invitation_code IS NOT NULL THEN um2.ap_account_id END) number_of_referral
			FROM analytics.users_master um
				LEFT JOIN analytics.users_master um2
				ON um.referral_code = um2.invitation_code 
			GROUP BY 1
		) r 
		ON u.ap_account_id = r.ap_account_id 
	LEFT JOIN aum_snapshot a 
		ON u.ap_account_id = a.ap_account_id 
		AND a.asset_type = 'ZMT'
	LEFT JOIN aum_snapshot aa 
		ON u.ap_account_id = aa.ap_account_id 
		AND aa.asset_type <> 'ZMT'	
WHERE 
	u.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001)
	AND u.is_verified = TRUE 
	AND u.signup_hostcountry IN ('TH','ID','AU','global')
--	AND u.ap_account_id = 143639
;


SELECT 
	um.ap_account_id 
	, COUNT( CASE WHEN um2.invitation_code IS NOT NULL THEN um2.ap_account_id END) referral_count
FROM analytics.users_master um
	LEFT JOIN analytics.users_master um2
	ON um.referral_code = um2.invitation_code 
GROUP BY 1,2


----- daily referral user by vendor (BananaIT/ COM7) 
WITH base AS (
SELECT DATE_TRUNC('day', u.inserted_at) register_date 
	, DATE_TRUNC('day', m.onfido_completed_at) verified_date 
	, DATE_TRUNC('day', m.zipup_subscribed_at) zipup_subscribed_date
	, DATE_TRUNC('day', m.first_traded_at) first_trade_date
	, m.ap_user_id user_id 
	, m.is_verified 
	, u.invitation_code 
	, u.signup_platform 
	, COUNT(DISTINCT m.user_id) register_user_count 
FROM user_app_public.users u 
	LEFT JOIN analytics.users_master m 
	ON u.id = m.user_id 
WHERE u.invitation_code IS NOT NULL 
GROUP BY 1,2,3,4,5,6,7,8
)
SELECT 
	DATE_TRUNC('day', b.register_date) datadate
	, b.invitation_code 
	, b.signup_platform 
	, SUM(register_user_count) register_user_count
	, COUNT(DISTINCT CASE WHEN is_verified = TRUE THEN user_id END) AS verified_user_count 
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('week', b.register_date) = DATE_TRUNC('week', b.verified_date) THEN b.user_id END) verified_same_week_count 
	, COUNT(DISTINCT CASE WHEN zipup_subscribed_date IS NOT NULL THEN user_id END) AS zipup_user_count 
	, COUNT(DISTINCT CASE WHEN first_trade_date IS NOT NULL THEN user_id END) AS traded_user_count 
	, "Store_Name" 
FROM base b 
	LEFT JOIN mappings.marketing_th_it_vendors c 
		ON b.invitation_code = c.store_referral_code
WHERE b.invitation_code IN (SELECT store_referral_code FROM mappings.marketing_th_it_vendors)
GROUP BY 1,2,3,9
ORDER BY 1 DESC 


----- list of referral user by vendor (BananaIT/ COM7) 
WITH base AS (
SELECT DATE_TRUNC('day', u.inserted_at) register_date 
	, DATE_TRUNC('day', m.onfido_completed_at) verified_date 
	, DATE_TRUNC('day', m.zipup_subscribed_at) zipup_subscribed_date
	, DATE_TRUNC('day', m.first_traded_at) first_trade_date
	, m.ap_user_id user_id 
	, m.email 
	, m.first_name
	, m.last_name 
	, m.is_verified 
	, u.invitation_code 
	, COUNT(DISTINCT m.user_id) register_user_count 
FROM user_app_public.users u 
	LEFT JOIN analytics.users_master m 
	ON u.id = m.user_id 
WHERE u.invitation_code IS NOT NULL 
GROUP BY 1,2,3,4,5,6,7,8,9,10 
)
SELECT 
	DATE_TRUNC('day', b.register_date) datadate  
	, b.invitation_code 
	, b.email 
	, b.first_name
	, b.last_name 
    , is_verified 
	, CASE WHEN zipup_subscribed_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_zipup_subscribed 
	, CASE WHEN first_trade_date IS NOT NULL THEN TRUE ELSE FALSE END AS has_traded  
	, CASE WHEN b.invitation_code = 'ID106' THEN 'ID106_Studio_7_Central_Rama2'
		WHEN b.invitation_code = 'ID109' THEN 'ID109_Studio_7_The_Mall_Bangkae'
		WHEN b.invitation_code = 'ID112' THEN 'ID112_Studio_7_Central_Bangna'
		WHEN b.invitation_code = 'ID114' THEN 'ID114_Studio_7_Central_Ladprao'
		WHEN b.invitation_code = 'ID115' THEN 'ID115_Studio_7_Future_Park_Rangsit'
		WHEN b.invitation_code = 'ID118' THEN 'ID118_Studio_7_The_Mall_Bangkapi'
		WHEN b.invitation_code = 'ID251' THEN 'ID251_Studio_7_Mega_Bangna'
		WHEN b.invitation_code = 'ID335' THEN 'ID335_Studio_7_Central_Pinklao'
		WHEN b.invitation_code = 'ID627' THEN 'ID627_Studio_7_Emquartier_Sukhumvit'
		WHEN b.invitation_code = 'ID645' THEN 'ID645_Studio_7_Central_Westgate'
		WHEN b.invitation_code = 'ID476' THEN 'ID476_BN_Central_Ladprao'
		WHEN b.invitation_code = 'ID179' THEN 'ID179_BN_Future_Park_Rangsit_3_1'
		WHEN b.invitation_code = 'ID167' THEN 'ID167_BN_Fashion_Ramintra_3_2'
		WHEN b.invitation_code = 'ID1075' THEN 'ID1075_BN_Market_Village_Suvanabhumi_2_1'
		WHEN b.invitation_code = 'ID1067' THEN 'ID1067_BN_Future_Park_Rangsit_2_1'
		WHEN b.invitation_code = 'ID182' THEN 'ID182_BN_Central_Pinklao'
		WHEN b.invitation_code = 'ID207' THEN 'ID207_BN_Central_Phuket'
		WHEN b.invitation_code = 'ID192' THEN 'ID192_BN_The Mall_Korat'
		WHEN b.invitation_code = 'ID458' THEN 'ID458_BNM_Central_Rama9'
		WHEN b.invitation_code = 'ID635' THEN 'ID635_BN_Central_Rayong'
		WHEN b.invitation_code = 'ID639' THEN 'ID639_BNM_Central_Chonbori'
		WHEN b.invitation_code = 'ID181' THEN 'ID181_BN_Central_Rama2'
		WHEN b.invitation_code = 'ID119' THEN 'ID119_BN_Central_Cheangwattana'
		WHEN b.invitation_code = 'ID459' THEN 'ID459_BN_Mega_Bangna'
	END AS store_name 
FROM base b 
WHERE b.invitation_code IN ('ID106',	'ID109',	'ID112',	'ID114',	'ID115',	'ID118',	'ID251',	'ID335'
,	'ID627',	'ID645',	'ID476',	'ID179',	'ID167',	'ID1075',	'ID1067',	'ID182',	'ID207',	'ID192'
,	'ID458',	'ID635',	'ID639',	'ID181',	'ID119',	'ID459')
ORDER BY 1 