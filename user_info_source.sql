-- kyc user not deposited
WITH has_deposited AS (
	SELECT 
		DISTINCT 
		dt.account_id 
	FROM 
		apex.deposit_tickets dt 
	WHERE 
		dt.account_id::TEXT IN (SELECT ap_account_id FROM mappings.commercial_adhoc_cs_team cact)
		AND dt.status = 5
--)	, has_traded AS (
--	SELECT 
--		DISTINCT 
--		ot.account_id 
--	FROM 
--		apex.oms_trades ot  
--	WHERE 
--		ot.account_id::TEXT IN (SELECT ap_account_id FROM mappings.commercial_adhoc_cs_team cact)
)	
SELECT 
	u.id user_id 
	, apu.ap_account_id 
	, u.email 
	, od.first_name 
	, od.last_name 
	, od.native_first_name
	, od.native_last_name
	, EXTRACT ('year' FROM AGE(NOW()::DATE, od.dob)) age_
	, oa.level_increase_status 
	, u.mobile_number 
	, u.invitation_code 
	, CASE WHEN dtm.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_deposited 
--	, CASE WHEN tm.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_traded 
	, ba.review_status bankbook_status
	, u.inserted_at::DATE register_date
FROM user_app_public.users u 
	LEFT JOIN user_app_public.alpha_point_users apu 
		ON u.id = apu.user_id 
	LEFT JOIN user_app_public.onfido_applicants oa 
		ON u.id = oa.user_id 
	LEFT JOIN user_app_public.onfido_documents od 
		ON u.id = od.user_id 
		AND od.archived_at IS NULL 
	LEFT JOIN user_app_public.bank_accounts ba 
		ON u.id = ba.user_id 
	LEFT JOIN has_deposited dtm 
		ON apu.ap_account_id = dtm.account_id 
--	LEFT JOIN has_traded tm 
--		ON apu.ap_account_id = tm.account_id 
WHERE 
	oa.updated_at >= DATE_TRUNC('month', NOW()::DATE - '1 month'::INTERVAL)
	AND u.inserted_at >= DATE_TRUNC('month', NOW()::DATE - '1 month'::INTERVAL)
	AND oa.level_increase_status = 'pass'
	AND dtm.account_id IS NULL
	AND u.signup_hostname = 'trade.zipmex.co.th'
;



-- kyc trade / deposit/ withdraw vol
WITH trade_vol AS (
	SELECT 
		tm.ap_account_id 
		, COUNT(DISTINCT tm.trade_id) count_trades 
		, SUM(tm.amount_usd) sum_trade_volume_usd
	FROM 
		analytics.trades_master tm 
	WHERE 
		tm.ap_account_id::TEXT IN (SELECT ap_account_id FROM mappings.commercial_adhoc_cs_team cact)
		AND tm.created_at >= '2022-03-01'
		AND tm.created_at < '2022-04-01'
	GROUP BY 1
)	, deposit_vol AS (
	SELECT 
		dm.ap_account_id 
		, COUNT(DISTINCT dm.ticket_id) count_deposits 
		, SUM(dm.amount_usd) sum_deposit_amount_usd
	FROM 
		analytics.deposit_tickets_master dm
	WHERE 
		dm.ap_account_id::TEXT IN (SELECT ap_account_id FROM mappings.commercial_adhoc_cs_team cact)
		AND tm.created_at >= '2022-03-01'
		AND tm.created_at < '2022-04-01'
	GROUP BY 1
)	, withdraw_vol AS (
	SELECT 
		wm.ap_account_id 
		, COUNT(DISTINCT wm.ticket_id) count_withdraws
		, SUM(wm.amount_usd) sum_withdraw_amount_usd
	FROM 
		analytics.withdraw_tickets_master wm
	WHERE 
		wm.ap_account_id::TEXT IN (SELECT ap_account_id FROM mappings.commercial_adhoc_cs_team cact)
		AND tm.created_at >= '2022-03-01'
		AND tm.created_at < '2022-04-01'
	GROUP BY 1
)
SELECT 
	um.ap_account_id 
	, um.level_increase_status 
	, um.base_fiat 
	, um.has_deposited 
	, d.count_deposits 
	, d.sum_deposit_amount_usd 
	, w.sum_withdraw_amount_usd 
	, um.has_traded 
	, t.count_trades 
	, t.sum_trade_volume_usd 
FROM analytics.users_master um 
	LEFT JOIN trade_vol t 
		ON um.ap_account_id = t.ap_account_id 
	LEFT JOIN deposit_vol d 
		ON um.ap_account_id = d.ap_account_id 
	LEFT JOIN withdraw_vol w 
		ON um.ap_account_id = w.ap_account_id 
WHERE 
	um.ap_account_id::TEXT IN (SELECT ap_account_id FROM mappings.commercial_adhoc_cs_team cact)
;




SELECT 
	up.email 
	, up.user_id 
	, up.ap_account_id 
	, up.mobile_number 
	, pi2.info ->> 'address_in_id_card_country' address_in_id_card_country
	, pi2.info ->> 'work_address_country' work_address_country
	, pi2.info ->> 'present_address_country' present_address_country
	, ss.survey ->> 'present_address_postal_code' present_address_postal_code
	, ss.survey ->> 'permanent_address' permanent_address
FROM analytics_pii.users_pii up 
	LEFT JOIN user_app_public.personal_infos pi2 
		ON up.user_id = pi2.user_id 
		AND pi2.archived_at IS NULL
	LEFT JOIN user_app_public.suitability_surveys ss 
		ON up.user_id = ss.user_id 
		AND ss.archived_at IS NULL
WHERE email IN ('')
;



-- global users using NRIC
SELECT 
	od.user_id 
	, up.email 
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
GROUP BY 1,2,3,4,5



