---- referral campaign funnel -- trade volume cohort
WITH base AS (
SELECT 
	DATE_TRUNC('week', u.created_at) register_week
	, DATE_TRUNC('week', u.onfido_completed_at) kyc_week
	, DATE_TRUNC('week', t.created_at) traded_week 
	, u.ap_account_id 
	, u.referring_user_id 
	, u.invitation_code 
	, user_id 
	, is_verified 
	, has_started_onfido 
	, is_onfido_verified 
	, is_email_verified 
	, is_mobile_verified 
	, t.symbol 
	, t.base_fiat 
	, SUM(t.amount_base_fiat) fiat_trade_volume
	, SUM(t.amount_usd) usd_trade_volume
FROM 
	analytics.users_master u 
	LEFT JOIN analytics.trades_master t 
		ON u.ap_account_id = t.ap_account_id 
--		AND DATE_TRUNC('day', u.created_at) = DATE_TRUNC('day', t.created_at) 
	LEFT JOIN user_app_public.users s 
		ON u.user_id = s.id 
WHERE u.signup_hostcountry IN ('AU','TH','ID','global')
AND u.ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227',27443
,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
), temp_ AS (
SELECT 
	DATE_TRUNC('week', register_week) dates 
--	, CASE 
--		WHEN invitation_code IN ('35KGD','35KSE','35KFA','35KXA','35KES','35KTT','35KMG','35KID','35KC2','35KSI','35KMV','35KIM') THEN 'paid_ads'
--		WHEN invitation_code NOT IN ('35KGD','35KSE','35KFA','35KXA','35KES','35KTT','35KMG','35KID','35KC2','35KSI','35KMV','35KIM') AND invitation_code IS NOT NULL THEN 'referral'
--		WHEN invitation_code IS NULL AND kyc_week = register_week THEN 'general'
--		ELSE 'organic' END AS "referral_campaign" 
	, COUNT(DISTINCT user_id) AS register_user 
	, COUNT(DISTINCT CASE WHEN is_verified = TRUE THEN user_id END) AS kyc_user 
	, COUNT(DISTINCT CASE WHEN traded_week = register_week AND fiat_trade_volume > 0 THEN user_id END) w0_active_rate
	, COUNT(DISTINCT CASE WHEN traded_week = register_week + '1 week'::INTERVAL AND fiat_trade_volume > 0 THEN user_id END) w1_active_rate
	, COUNT(DISTINCT CASE WHEN traded_week = register_week + '2 week'::INTERVAL AND fiat_trade_volume > 0 THEN user_id END) w2_active_rate
	, COUNT(DISTINCT CASE WHEN traded_week = register_week + '3 week'::INTERVAL AND fiat_trade_volume > 0 THEN user_id END) w3_active_rate
	, COUNT(DISTINCT CASE WHEN traded_week = register_week + '4 week'::INTERVAL AND fiat_trade_volume > 0 THEN user_id END) w4_active_rate
	, COUNT(DISTINCT CASE WHEN traded_week = register_week + '5 week'::INTERVAL AND fiat_trade_volume > 0 THEN user_id END) w5_active_rate
	, COUNT(DISTINCT CASE WHEN traded_week = register_week + '6 week'::INTERVAL AND fiat_trade_volume > 0 THEN user_id END) w6_active_rate
	, COUNT(DISTINCT CASE WHEN traded_week = register_week + '7 week'::INTERVAL AND fiat_trade_volume > 0 THEN user_id END) w7_active_rate
	, COUNT(DISTINCT CASE WHEN traded_week = register_week + '8 week'::INTERVAL AND fiat_trade_volume > 0 THEN user_id END) w8_active_rate
	, COUNT(DISTINCT CASE WHEN traded_week = register_week + '9 week'::INTERVAL AND fiat_trade_volume > 0 THEN user_id END) w9_active_rate
	, COUNT(DISTINCT CASE WHEN traded_week = register_week + '10 week'::INTERVAL AND fiat_trade_volume > 0 THEN user_id END) w10_active_rate
FROM 
	base 
GROUP BY 1
)
SELECT 
	dates
--	, referral_campaign 
	, register_user 
	, CASE WHEN register_user = 0 THEN 0 ELSE kyc_user / register_user::float END AS kyc_rate 
	, CASE WHEN kyc_user = 0 THEN 0 ELSE w0_active_rate / kyc_user::float END AS w0_active_rate
	, CASE WHEN kyc_user = 0 THEN 0 ELSE w1_active_rate / kyc_user::float END AS w1_active_rate
	, CASE WHEN kyc_user = 0 THEN 0 ELSE w2_active_rate / kyc_user::float END AS w2_active_rate
	, CASE WHEN kyc_user = 0 THEN 0 ELSE w3_active_rate / kyc_user::float END AS w3_active_rate
	, CASE WHEN kyc_user = 0 THEN 0 ELSE w4_active_rate / kyc_user::float END AS w4_active_rate
	, CASE WHEN kyc_user = 0 THEN 0 ELSE w5_active_rate / kyc_user::float END AS w5_active_rate
	, CASE WHEN kyc_user = 0 THEN 0 ELSE w6_active_rate / kyc_user::float END AS w6_active_rate
	, CASE WHEN kyc_user = 0 THEN 0 ELSE w7_active_rate / kyc_user::float END AS w7_active_rate
	, CASE WHEN kyc_user = 0 THEN 0 ELSE w8_active_rate / kyc_user::float END AS w8_active_rate
	, CASE WHEN kyc_user = 0 THEN 0 ELSE w9_active_rate / kyc_user::float END AS w9_active_rate
	, CASE WHEN kyc_user = 0 THEN 0 ELSE w10_active_rate / kyc_user::float END AS w10_active_rate
FROM 
	temp_ 
WHERE 
	dates >= '2021-05-31 00:00:00'
ORDER BY 1 DESC 
;


SELECT *
FROM analytics.deposit_tickets_master dtm 
WHERE ap_account_id = 407841
ORDER BY created_at DESC 



-- avg min max deposit volume
WITH temp_deposit AS (
SELECT 
	Date_trunc('day', u.created_at) created_at
	, d.signup_hostcountry 
	, d.ap_account_id 
	, SUM(d.amount_base_fiat) amount_base_fiat 
	, SUM(d.amount_usd) amount_usd 
FROM 
	analytics.deposit_tickets_master d 
	LEFT JOIN analytics.users_master u 
		ON d.ap_account_id = u.ap_account_id 
WHERE 
	status = 'FullyProcessed'
	AND d.signup_hostcountry = 'ID'
GROUP BY 1,2,3
), base_7 AS (
SELECT 
	 'Last 07 day' "period" 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_deposit 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '7 day'::INTERVAL 
GROUP BY 1,2
), base_7m AS (
SELECT 
	"period" 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_7 
GROUP BY 1 
), t7_deposit_ AS (
SELECT 
	b."period" 
	, m.amount_base_fiat/ m.user_count avg_deposit_fiat
	, m.amount_usd / m.user_count avg_deposit_usd 
	, MIN(b.amount_base_fiat) min_deposit_fiat 
	, MIN(b.amount_usd) min_deposit_usd
	, MAX(b.amount_base_fiat) max_deposit_fiat
	, MAX(b.amount_usd) max_deposit_usd 
FROM base_7 b 
	LEFT JOIN base_7m m ON b."period" = m."period" 
GROUP BY 1,2,3 
), base_14 AS (
SELECT 
	 'Last 14 day' "period" 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_deposit 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '14 day'::INTERVAL 
GROUP BY 1,2
), base_14m AS (
SELECT 
	"period" 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_14 
GROUP BY 1 
), t14_deposit_ AS (
SELECT 
	b."period" 
	, m.amount_base_fiat/ m.user_count avg_deposit_fiat
	, m.amount_usd / m.user_count avg_deposit_usd 
	, MIN(b.amount_base_fiat) min_deposit_fiat 
	, MIN(b.amount_usd) min_deposit_usd
	, MAX(b.amount_base_fiat) max_deposit_fiat
	, MAX(b.amount_usd) max_deposit_usd 
FROM base_14 b 
	LEFT JOIN base_14m m ON b."period" = m."period" 
GROUP BY 1,2,3 
), base_30 AS (
SELECT 
	 'Last 30 day' "period" 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_deposit 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '30 day'::INTERVAL 
GROUP BY 1,2
), base_30m AS (
SELECT 
	"period" 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_30 
GROUP BY 1 
), t30_deposit_ AS (
SELECT 
	b."period" 
	, m.amount_base_fiat/ m.user_count avg_deposit_fiat
	, m.amount_usd / m.user_count avg_deposit_usd 
	, MIN(b.amount_base_fiat) min_deposit_fiat 
	, MIN(b.amount_usd) min_deposit_usd
	, MAX(b.amount_base_fiat) max_deposit_fiat
	, MAX(b.amount_usd) max_deposit_usd 
FROM base_30 b 
	LEFT JOIN base_30m m ON b."period" = m."period" 
GROUP BY 1,2,3
-- avg min max withdraw volume
), temp_withdraw AS (
SELECT 
	Date_trunc('day', u.created_at) created_at
	, w.signup_hostcountry 
	, w.ap_account_id 
	, SUM(w.amount_base_fiat) amount_base_fiat 
	, SUM(w.amount_usd) amount_usd 
FROM 
	analytics.withdraw_tickets_master w  
	LEFT JOIN analytics.users_master u 
		ON w.ap_account_id = u.ap_account_id 
WHERE 
	status = 'FullyProcessed'
	AND w.signup_hostcountry = 'ID'
GROUP BY 1,2,3
), base_w7 AS (
SELECT 
	 'Last 07 day' "period" 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_withdraw 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '7 day'::INTERVAL 
GROUP BY 1,2
), base_w7m AS (
SELECT 
	"period" 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_w7 
GROUP BY 1 
), t7_withdraw_ AS (
SELECT 
	b."period" 
	, m.amount_base_fiat/ m.user_count avg_withdraw_fiat
	, m.amount_usd / m.user_count avg_withdraw_usd 
	, MIN(b.amount_base_fiat) min_withdraw_fiat 
	, MIN(b.amount_usd) min_withdraw_usd
	, MAX(b.amount_base_fiat) max_withdraw_fiat
	, MAX(b.amount_usd) max_withdraw_usd 
FROM base_w7 b 
	LEFT JOIN base_w7m m ON b."period" = m."period" 
GROUP BY 1,2,3 
), base_w14 AS (
SELECT 
	 'Last 14 day' "period" 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_withdraw 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '14 day'::INTERVAL 
GROUP BY 1,2
), base_w14m AS (
SELECT 
	"period" 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_w14 
GROUP BY 1 
), t14_withdraw_ AS (
SELECT 
	b."period" 
	, m.amount_base_fiat/ m.user_count avg_deposit_fiat
	, m.amount_usd / m.user_count avg_deposit_usd 
	, MIN(b.amount_base_fiat) min_deposit_fiat 
	, MIN(b.amount_usd) min_deposit_usd
	, MAX(b.amount_base_fiat) max_deposit_fiat
	, MAX(b.amount_usd) max_deposit_usd 
FROM base_w14 b 
	LEFT JOIN base_w14m m ON b."period" = m."period" 
GROUP BY 1,2,3 
), base_w30 AS (
SELECT 
	 'Last 30 day' "period" 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_withdraw 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '30 day'::INTERVAL 
GROUP BY 1,2
), base_w30m AS (
SELECT 
	"period" 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_w30 
GROUP BY 1 
), t30_withdraw_ AS (
SELECT 
	b."period" 
	, m.amount_base_fiat/ m.user_count avg_withdraw_fiat
	, m.amount_usd / m.user_count avg_withdraw_usd 
	, MIN(b.amount_base_fiat) min_withdraw_fiat 
	, MIN(b.amount_usd) min_withdraw_usd
	, MAX(b.amount_base_fiat) max_withdraw_fiat
	, MAX(b.amount_usd) max_withdraw_usd 
FROM base_w30 b 
	LEFT JOIN base_w30m m ON b."period" = m."period" 
GROUP BY 1,2,3
), final_w AS (
SELECT * FROM t7_withdraw_
UNION ALL
SELECT * FROM t14_withdraw_
UNION ALL
SELECT * FROM t30_withdraw_
), final_d AS (
SELECT * FROM t7_deposit_
UNION ALL
SELECT * FROM t14_deposit_
UNION ALL
SELECT * FROM t30_deposit_
)
-- avg min max trading volume
, temp_trade AS (
SELECT 
	Date_trunc('day', u.created_at) created_at
	, t.signup_hostcountry 
	, t.ap_account_id 
	, SUM(t.amount_base_fiat) amount_base_fiat 
	, SUM(t.amount_usd) amount_usd 
FROM 
	analytics.trades_master t  
	LEFT JOIN analytics.users_master u 
		ON t.ap_account_id = u.ap_account_id 
WHERE 
	t.ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227',27443
	,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347')
	AND t.signup_hostcountry = 'ID'
GROUP BY 1,2,3
), base_t7 AS (
SELECT 
	 'Last 07 day' "period" 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_trade 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '7 day'::INTERVAL 
GROUP BY 1,2
), base_t7m AS (
SELECT 
	"period" 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_t7 
GROUP BY 1 
), t7_trade_ AS (
SELECT 
	b."period" 
	, m.amount_base_fiat/ m.user_count avg_trade_fiat
	, m.amount_usd / m.user_count avg_trade_usd 
	, MIN(b.amount_base_fiat) min_trade_fiat 
	, MIN(b.amount_usd) min_trade_usd
	, MAX(b.amount_base_fiat) max_trade_fiat
	, MAX(b.amount_usd) max_trade_usd 
FROM base_t7 b 
	LEFT JOIN base_t7m m ON b."period" = m."period" 
GROUP BY 1,2,3 
), base_t14 AS (
SELECT 
	 'Last 14 day' "period" 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_trade 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '14 day'::INTERVAL 
GROUP BY 1,2
), base_t14m AS (
SELECT 
	"period" 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_t14 
GROUP BY 1 
), t14_trade_ AS (
SELECT 
	b."period" 
	, m.amount_base_fiat/ m.user_count avg_trade_fiat
	, m.amount_usd / m.user_count avg_trade_usd 
	, MIN(b.amount_base_fiat) min_trade_fiat 
	, MIN(b.amount_usd) min_trade_usd
	, MAX(b.amount_base_fiat) max_trade_fiat
	, MAX(b.amount_usd) max_trade_usd 
FROM base_t14 b 
	LEFT JOIN base_t14m m ON b."period" = m."period" 
GROUP BY 1,2,3 
), base_t30 AS (
SELECT 
	 'Last 30 day' "period" 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_trade 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '30 day'::INTERVAL 
GROUP BY 1,2
), base_t30m AS (
SELECT 
	"period" 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_t30 
GROUP BY 1 
), t30_trade_ AS (
SELECT 
	b."period" 
	, m.amount_base_fiat/ m.user_count avg_trade_fiat
	, m.amount_usd / m.user_count avg_trade_usd 
	, MIN(b.amount_base_fiat) min_trade_fiat 
	, MIN(b.amount_usd) min_trade_usd
	, MAX(b.amount_base_fiat) max_trade_fiat
	, MAX(b.amount_usd) max_trade_usd 
FROM base_t30 b 
	LEFT JOIN base_t30m m ON b."period" = m."period" 
GROUP BY 1,2,3
), final_t AS (
SELECT * FROM t7_trade_
UNION ALL
SELECT * FROM t14_trade_
UNION ALL
SELECT * FROM t30_trade_
)
SELECT 
	d."period"
	, avg_deposit_fiat	, avg_withdraw_fiat	, avg_trade_fiat
	, min_deposit_fiat	, min_withdraw_fiat	, min_trade_fiat
	, max_deposit_fiat	, max_withdraw_fiat	, max_trade_fiat
	, avg_deposit_usd	, avg_withdraw_usd	, avg_trade_usd
	, min_deposit_usd	, min_withdraw_usd	, min_trade_usd
	, max_deposit_usd	, max_withdraw_usd 	, max_trade_usd
FROM final_d d 
	LEFT JOIN final_w w ON d."period" = w."period" 
	LEFT JOIN final_t t ON d."period" = t."period" 
;



-- campaign specific -- avg min max deposit volume
WITH temp_deposit AS ( 
SELECT 
	Date_trunc('day', u.created_at) created_at 
	, d.signup_hostcountry 
	, d.ap_account_id 
	, CASE 
		WHEN u.invitation_code IN ('35KGD','35KSE','35KFA','35KXA','35KES','35KTT','35KMG','35KID','35KC2','35KSI','35KMV','35KIM') THEN 'paid_ads'
		WHEN u.invitation_code NOT IN ('35KGD','35KSE','35KFA','35KXA','35KES','35KTT','35KMG','35KID','35KC2','35KSI','35KMV','35KIM') AND u.invitation_code IS NOT NULL THEN 'referral'
		WHEN u.invitation_code IS NULL AND DATE_TRUNC('week', u.onfido_completed_at) <= DATE_TRUNC('week', u.created_at) THEN 'general'
		ELSE 'organic' END AS "referral_campaign" 
	, SUM(d.amount_base_fiat) amount_base_fiat 
	, SUM(d.amount_usd) amount_usd 
FROM 
	analytics.deposit_tickets_master d 
LEFT JOIN 
	analytics.users_master u 
	ON d.ap_account_id = u.ap_account_id 
LEFT JOIN 
	user_app_public.users s 
	ON u.user_id = s.id 
WHERE 
	status = 'FullyProcessed'
	AND d.signup_hostcountry = 'ID'
GROUP BY 1,2,3,4
), base_7 AS (
SELECT 
	 'Last 07 day' "period" 
	, referral_campaign 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_deposit 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '7 day'::INTERVAL 
GROUP BY 1,2,3
), base_7m AS (
SELECT 
	"period" 
	, referral_campaign 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_7 
GROUP BY 1,2 
), t7_deposit_ AS (
SELECT 
	b."period" 
	, b.referral_campaign 
	, m.amount_base_fiat/ m.user_count avg_deposit_fiat
	, m.amount_usd / m.user_count avg_deposit_usd 
	, MIN(b.amount_base_fiat) min_deposit_fiat 
	, MIN(b.amount_usd) min_deposit_usd
	, MAX(b.amount_base_fiat) max_deposit_fiat
	, MAX(b.amount_usd) max_deposit_usd 
FROM base_7 b 
	LEFT JOIN base_7m m ON b."period" = m."period" AND b.referral_campaign = m.referral_campaign
GROUP BY 1,2,3,4 
), base_14 AS (
SELECT 
	 'Last 14 day' "period" 
	, referral_campaign 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_deposit 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '14 day'::INTERVAL 
GROUP BY 1,2,3
), base_14m AS (
SELECT 
	"period" 
	, referral_campaign 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_14 
GROUP BY 1,2 
), t14_deposit_ AS (
SELECT 
	b."period" 
	, b.referral_campaign 
	, m.amount_base_fiat/ m.user_count avg_deposit_fiat
	, m.amount_usd / m.user_count avg_deposit_usd 
	, MIN(b.amount_base_fiat) min_deposit_fiat 
	, MIN(b.amount_usd) min_deposit_usd
	, MAX(b.amount_base_fiat) max_deposit_fiat
	, MAX(b.amount_usd) max_deposit_usd 
FROM base_14 b 
	LEFT JOIN base_14m m ON b."period" = m."period" AND b.referral_campaign = m.referral_campaign
GROUP BY 1,2,3,4 
), base_30 AS (
SELECT 
	 'Last 30 day' "period" 
	, referral_campaign 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_deposit 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '30 day'::INTERVAL 
GROUP BY 1,2,3
), base_30m AS (
SELECT 
	"period" 
	, referral_campaign 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_30 
GROUP BY 1,2
), t30_deposit_ AS (
SELECT 
	b."period" 
	, b.referral_campaign 
	, m.amount_base_fiat/ m.user_count avg_deposit_fiat
	, m.amount_usd / m.user_count avg_deposit_usd 
	, MIN(b.amount_base_fiat) min_deposit_fiat 
	, MIN(b.amount_usd) min_deposit_usd
	, MAX(b.amount_base_fiat) max_deposit_fiat
	, MAX(b.amount_usd) max_deposit_usd 
FROM base_30 b 
	LEFT JOIN base_30m m ON b."period" = m."period" AND b.referral_campaign = m.referral_campaign
GROUP BY 1,2,3,4
ORDER BY 1,2
), final_d AS (
SELECT * FROM t7_deposit_
UNION ALL
SELECT * FROM t14_deposit_
UNION ALL
SELECT * FROM t30_deposit_ 
)
-- campaign specific -- avg min max withdraw volume
, temp_withdraw AS (
SELECT 
	Date_trunc('day', u.created_at) created_at
	, w.signup_hostcountry 
	, w.ap_account_id 
	, CASE 
		WHEN u.invitation_code IN ('35KGD','35KSE','35KFA','35KXA','35KES','35KTT','35KMG','35KID','35KC2','35KSI','35KMV','35KIM') THEN 'paid_ads'
		WHEN u.invitation_code NOT IN ('35KGD','35KSE','35KFA','35KXA','35KES','35KTT','35KMG','35KID','35KC2','35KSI','35KMV','35KIM') AND u.invitation_code IS NOT NULL THEN 'referral'
		WHEN u.invitation_code IS NULL AND DATE_TRUNC('day', u.onfido_completed_at) <= DATE_TRUNC('day', u.created_at) THEN 'general'
		ELSE 'organic' END AS "referral_campaign" 
	, SUM(w.amount_base_fiat) amount_base_fiat 
	, SUM(w.amount_usd) amount_usd 
FROM 
	analytics.withdraw_tickets_master w 
LEFT JOIN 
	analytics.users_master u 
	ON w.ap_account_id = u.ap_account_id 
LEFT JOIN 
	user_app_public.users s 
	ON u.user_id = s.id 
WHERE 
	status = 'FullyProcessed'
	AND w.signup_hostcountry = 'ID'
GROUP BY 1,2,3,4
), base_w7 AS (
SELECT 
	 'Last 07 day' "period" 
	, referral_campaign 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_withdraw 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '7 day'::INTERVAL 
GROUP BY 1,2,3
), base_w7m AS (
SELECT 
	"period" 
	, referral_campaign 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_w7 
GROUP BY 1,2 
), t7_withdraw_ AS (
SELECT 
	b."period" 
	, b.referral_campaign 
	, m.amount_base_fiat/ m.user_count avg_withdraw_fiat
	, m.amount_usd / m.user_count avg_withdraw_usd 
	, MIN(b.amount_base_fiat) min_withdraw_fiat 
	, MIN(b.amount_usd) min_withdraw_usd
	, MAX(b.amount_base_fiat) max_withdraw_fiat
	, MAX(b.amount_usd) max_withdraw_usd 
FROM base_w7 b 
	LEFT JOIN base_w7m m ON b."period" = m."period" AND b.referral_campaign = m.referral_campaign
GROUP BY 1,2,3,4 
), base_w14 AS (
SELECT 
	 'Last 14 day' "period" 
	, referral_campaign 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_withdraw 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '14 day'::INTERVAL 
GROUP BY 1,2,3
), base_w14m AS (
SELECT 
	"period" 
	, referral_campaign 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_w14 
GROUP BY 1,2 
), t14_withdraw_ AS (
SELECT 
	b."period" 
	, b.referral_campaign 
	, m.amount_base_fiat/ m.user_count avg_withdraw_fiat
	, m.amount_usd / m.user_count avg_withdraw_usd 
	, MIN(b.amount_base_fiat) min_withdraw_fiat 
	, MIN(b.amount_usd) min_withdraw_usd
	, MAX(b.amount_base_fiat) max_withdraw_fiat
	, MAX(b.amount_usd) max_withdraw_usd 
FROM base_w14 b 
	LEFT JOIN base_w14m m ON b."period" = m."period" AND b.referral_campaign = m.referral_campaign
GROUP BY 1,2,3,4 
), base_w30 AS (
SELECT 
	 'Last 30 day' "period" 
	, referral_campaign 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_withdraw 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '30 day'::INTERVAL 
GROUP BY 1,2,3
), base_w30m AS (
SELECT 
	"period" 
	, referral_campaign 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_w30 
GROUP BY 1,2
), t30_withdraw_ AS (
SELECT 
	b."period" 
	, b.referral_campaign 
	, m.amount_base_fiat/ m.user_count avg_withdraw_fiat
	, m.amount_usd / m.user_count avg_withdraw_usd 
	, MIN(b.amount_base_fiat) min_withdraw_fiat 
	, MIN(b.amount_usd) min_withdraw_usd
	, MAX(b.amount_base_fiat) max_withdraw_fiat
	, MAX(b.amount_usd) max_withdraw_usd 
FROM base_w30 b 
	LEFT JOIN base_w30m m ON b."period" = m."period" AND b.referral_campaign = m.referral_campaign
GROUP BY 1,2,3,4
ORDER BY 1,2
), final_w AS (
SELECT * FROM t7_withdraw_
UNION ALL
SELECT * FROM t14_withdraw_
UNION ALL
SELECT * FROM t30_withdraw_
)
-- campaign specific -- avg min max trading volume
, temp_trade AS (
SELECT 
	Date_trunc('day', u.created_at) created_at
	, t.signup_hostcountry 
	, t.ap_account_id 
	, CASE 
		WHEN u.invitation_code IN ('35KGD','35KSE','35KFA','35KXA','35KES','35KTT','35KMG','35KID','35KC2','35KSI','35KMV','35KIM') THEN 'paid_ads'
		WHEN u.invitation_code NOT IN ('35KGD','35KSE','35KFA','35KXA','35KES','35KTT','35KMG','35KID','35KC2','35KSI','35KMV','35KIM') AND u.invitation_code IS NOT NULL THEN 'referral'
		WHEN u.invitation_code IS NULL AND DATE_TRUNC('day', u.onfido_completed_at) <= DATE_TRUNC('day', u.created_at) THEN 'general'
		ELSE 'organic' END AS "referral_campaign" 
	, SUM(t.amount_base_fiat) amount_base_fiat 
	, SUM(t.amount_usd) amount_usd 
FROM 
	analytics.trades_master t  
LEFT JOIN 
	analytics.users_master u 
	ON t.ap_account_id = u.ap_account_id 
LEFT JOIN 
	user_app_public.users s 
	ON u.user_id = s.id 
WHERE 
	t.ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227',27443
	,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347')
	AND t.signup_hostcountry = 'ID'
GROUP BY 1,2,3,4
), base_t7 AS (
SELECT 
	 'Last 07 day' "period" 
	, referral_campaign 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_trade 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '7 day'::INTERVAL 
GROUP BY 1,2,3
), base_t7m AS (
SELECT 
	"period" 
	, referral_campaign 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_t7 
GROUP BY 1,2 
), t7_trade_ AS (
SELECT 
	b."period" 
	, b.referral_campaign 
	, m.amount_base_fiat/ m.user_count avg_trade_fiat
	, m.amount_usd / m.user_count avg_trade_usd 
	, MIN(b.amount_base_fiat) min_trade_fiat 
	, MIN(b.amount_usd) min_trade_usd
	, MAX(b.amount_base_fiat) max_trade_fiat
	, MAX(b.amount_usd) max_trade_usd 
FROM base_t7 b 
	LEFT JOIN base_t7m m ON b."period" = m."period" AND b.referral_campaign = m.referral_campaign
GROUP BY 1,2,3,4 
), base_t14 AS (
SELECT 
	 'Last 14 day' "period" 
	, referral_campaign 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_trade 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '14 day'::INTERVAL 
GROUP BY 1,2,3
), base_t14m AS (
SELECT 
	"period" 
	, referral_campaign 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_t14 
GROUP BY 1,2 
), t14_trade_ AS (
SELECT 
	b."period" 
	, b.referral_campaign 
	, m.amount_base_fiat/ m.user_count avg_trade_fiat
	, m.amount_usd / m.user_count avg_trade_usd 
	, MIN(b.amount_base_fiat) min_trade_fiat 
	, MIN(b.amount_usd) min_trade_usd
	, MAX(b.amount_base_fiat) max_trade_fiat
	, MAX(b.amount_usd) max_trade_usd 
FROM base_t14 b 
	LEFT JOIN base_t14m m ON b."period" = m."period" AND b.referral_campaign = m.referral_campaign
GROUP BY 1,2,3,4 
), base_t30 AS (
SELECT 
	 'Last 30 day' "period" 
	, referral_campaign 
	, ap_account_id 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
FROM 
	temp_trade 
WHERE 
	Date_trunc('day', created_at) >= date_trunc('day', NOW()) - '30 day'::INTERVAL 
GROUP BY 1,2,3
), base_t30m AS (
SELECT 
	"period" 
	, referral_campaign 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd
FROM base_t30 
GROUP BY 1,2
), t30_trade_ AS (
SELECT 
	b."period" 
	, b.referral_campaign 
	, m.amount_base_fiat/ m.user_count avg_trade_fiat
	, m.amount_usd / m.user_count avg_trade_usd 
	, MIN(b.amount_base_fiat) min_trade_fiat 
	, MIN(b.amount_usd) min_trade_usd
	, MAX(b.amount_base_fiat) max_trade_fiat
	, MAX(b.amount_usd) max_trade_usd 
FROM base_t30 b 
	LEFT JOIN base_t30m m ON b."period" = m."period" AND b.referral_campaign = m.referral_campaign
GROUP BY 1,2,3,4
ORDER BY 1,2
), final_t AS (
SELECT * FROM t7_trade_
UNION ALL
SELECT * FROM t14_trade_
UNION ALL
SELECT * FROM t30_trade_
)
SELECT 
	d.referral_campaign
	, d."period"
	, avg_deposit_fiat	, avg_withdraw_fiat	, avg_trade_fiat
	, min_deposit_fiat	, min_withdraw_fiat	, min_trade_fiat
	, max_deposit_fiat	, max_withdraw_fiat	, max_trade_fiat
	, avg_deposit_usd	, avg_withdraw_usd	, avg_trade_usd
	, min_deposit_usd	, min_withdraw_usd	, min_trade_usd
	, max_deposit_usd	, max_withdraw_usd 	, max_trade_usd
FROM final_d d 
	LEFT JOIN final_w w ON d."period" = w."period" AND d.referral_campaign = w.referral_campaign
	LEFT JOIN final_t t ON d."period" = t."period" AND d.referral_campaign = t.referral_campaign 
ORDER BY 1,2
;



-- average portfolio value per user on 7/14/30 day after register 
WITH daily_balance AS (
SELECT 
	DATE_TRUNC('day', a.created_at) created_at 
	, DATE_TRUNC('day', u.created_at) register_date 
	, u.signup_hostcountry 
	, a.account_id 
	, CASE 
		WHEN u.invitation_code IN ('35KGD','35KSE','35KFA','35KXA','35KES','35KTT','35KMG','35KID','35KC2','35KSI','35KMV','35KIM') THEN 'paid_ads'
		WHEN u.invitation_code NOT IN ('35KGD','35KSE','35KFA','35KXA','35KES','35KTT','35KMG','35KID','35KC2','35KSI','35KMV','35KIM') AND u.invitation_code IS NOT NULL THEN 'referral'
		WHEN u.invitation_code IS NULL AND DATE_TRUNC('week', u.onfido_completed_at) <= DATE_TRUNC('week', u.created_at) THEN 'general'
		ELSE 'organic' END AS "referral_campaign" 
	, e.product_2_symbol fiat 
	, e2.exchange_rate usdidr_fx 
	, SUM(amount) coin_balance
	, SUM(CASE WHEN a.product_id = 6 THEN a.amount * 1
			ELSE a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END) usd_amount
FROM public.accounts_positions_daily a 
	LEFT JOIN analytics.users_master u
		ON a.account_id = u.ap_account_id 
	LEFT JOIN user_app_public.users s 
		ON s.id = u.user_id 
	LEFT JOIN apex.products p 
		ON a.product_id = p.product_id
	LEFT JOIN oms_data_public.cryptocurrency_prices c 
	    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
	    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
	LEFT JOIN public.daily_closing_gold_prices g
		ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
		AND a.product_id IN (15, 35)
	LEFT JOIN public.daily_ap_prices z
		ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
		AND z.instrument_symbol  = 'ZMTUSD'
		AND a.product_id in (16, 50)
	LEFT JOIN oms_data_public.exchange_rates e
		ON DATE_TRUNC('day', e.created_at) = DATE_TRUNC('day', a.created_at)
		AND e.product_2_symbol  = p.symbol
		AND e.source = 'coinmarketcap'
	LEFT JOIN oms_data_public.exchange_rates e2
		ON DATE_TRUNC('day', e2.created_at) = DATE_TRUNC('day', a.created_at)
		AND e2.product_2_symbol  = 'IDR'
		AND e2.source = 'coinmarketcap'
WHERE u.signup_hostcountry IN ('ID') --('AU','TH','ID','global') 
AND account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227',27443
,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347')
AND DATE_TRUNC('day', a.created_at) < DATE_TRUNC('day', NOW())
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1 DESC 
), idr_balance AS (
SELECT 
	created_at 
	, register_date 
	, account_id 
	, referral_campaign 
	, usdidr_fx 
	, SUM(usd_amount) usd_amount 
	, SUM(CASE WHEN fiat = 'IDR' THEN coin_balance * 1 ELSE COALESCE(usd_amount,0) * usdidr_fx END) AS idr_amount 
FROM daily_balance 
GROUP BY 1,2,3,4,5
ORDER BY 1 DESC
), t7_balance AS (
SELECT 
	register_date 
	, created_at balance_at
	, '7 day' "period"
--	, referral_campaign
	, COUNT(DISTINCT account_id) user_count 
	, SUM(usd_amount) usd_amount
	, SUM(idr_amount) idr_amount 
FROM idr_balance 
WHERE created_at = DATE_TRUNC('day', register_date) + '7 day'::INTERVAL 
AND register_date >= '2021-06-01 00:00:00'
GROUP BY 1,2,3
), t14_balance AS (
SELECT 
	register_date 
	, created_at balance_at
	, '14 day' "period"
--	, referral_campaign
	, COUNT(DISTINCT account_id) user_count 
	, SUM(usd_amount) usd_amount
	, SUM(idr_amount) idr_amount 
FROM idr_balance
WHERE created_at = DATE_TRUNC('day', register_date) + '14 day'::INTERVAL 
AND register_date >= '2021-06-01 00:00:00'
GROUP BY 1,2,3
), t30_balance AS (
SELECT 
	register_date 
	, created_at balance_at
	, '30 day' "period"
--	, referral_campaign
	, COUNT(DISTINCT account_id) user_count 
	, SUM(usd_amount) usd_amount
	, SUM(idr_amount) idr_amount 
FROM idr_balance
WHERE created_at = DATE_TRUNC('day', register_date) + '30 day'::INTERVAL 
AND register_date >= '2021-06-01 00:00:00'
GROUP BY 1,2,3
)
SELECT *, usd_amount/ user_count avg_usd_balance, idr_amount/ user_count avg_ird_balance FROM t7_balance 
UNION ALL 
SELECT *, usd_amount/ user_count avg_usd_balance, idr_amount/ user_count avg_ird_balance FROM t14_balance 
UNION ALL 
SELECT *, usd_amount/ user_count avg_usd_balance, idr_amount/ user_count avg_ird_balance FROM t30_balance 


-- app-install-id --> register --> kyc submitted --> kyc passed --> deposit --> trade 
WITH base AS (
SELECT 
	r.user_id ga_user_id 
	, u.user_id um_user_id
	, u.email 
	, u.ap_account_id 
	, u.signup_hostcountry 
	, r.platform 
	, r.landing_page_affliate_code 
	, CASE 
		WHEN landing_page_affliate_code IN ('35KGD','35KSE','35KFA','35KXA','35KES','35KTT','35KMG','35KID','35KC2','35KSI','35KMV','35KIM') THEN 'paid_ads'
		WHEN landing_page_affliate_code NOT IN ('35KGD','35KSE','35KFA','35KXA','35KES','35KTT','35KMG','35KID','35KC2','35KSI','35KMV','35KIM') AND landing_page_affliate_code IS NOT NULL THEN 'referral'
		WHEN landing_page_affliate_code IS NULL AND DATE_TRUNC('week', u.onfido_completed_at) <= DATE_TRUNC('week', u.created_at) THEN 'general'
		WHEN landing_page_affliate_code IS NULL AND DATE_TRUNC('week', u.onfido_completed_at) > DATE_TRUNC('week', u.created_at) THEN 'organic'
		ELSE 'N/A' END AS "referral_campaign" 
	, DATE_TRUNC('day', r.datetime_singapore - '6 hour'::INTERVAL) app_install_date 
	, DATE_TRUNC('day', u.created_at) register_date 
	, DATE_TRUNC('day', u.email_verified_at) email_verified_at 
	, DATE_TRUNC('day', u.mobile_verified_at) mobile_verified_at 
	, DATE_TRUNC('day', COALESCE(p.inserted_at, d.inserted_at)) kyc_start 
	, o.level_increase_status 
	, DATE_TRUNC('day', o.updated_at) kyc_passed 
	, u.has_deposited 
	, u.has_traded 
FROM analytics.users_master u 
	LEFT JOIN 
		analytics.registration_channel_master r 
		ON r.user_id = u.user_id 
	LEFT JOIN 
		( SELECT user_id , inserted_at , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY inserted_at) row_ 
		FROM user_app_public.personal_infos
		) p 
		ON u.user_id = p.user_id 
		AND p.row_ = 1  
	LEFT JOIN 
		( SELECT applicant_id , inserted_at , ROW_NUMBER() OVER(PARTITION BY applicant_id ORDER BY inserted_at DESC) row_ 
		FROM user_app_public.onfido_documents
		) d 
		ON u.onfido_applicant_id = d.applicant_id 
		AND d.row_ = 1 
	LEFT JOIN user_app_public.onfido_applicants o
		ON o.user_id = u.user_id 
WHERE u.signup_hostcountry IN ('ID','AU','TH','global')
)
SELECT 
	register_date
	, signup_hostcountry 
	, platform 
	, referral_campaign 
	, COUNT(DISTINCT um_user_id) register_count
	, COUNT(DISTINCT CASE WHEN platform <> 'WEB' THEN ga_user_id END) AS app_install_count
	, COUNT(DISTINCT CASE WHEN email_verified_at IS NOT NULL THEN um_user_id END) AS email_verified_count
	, COUNT(DISTINCT CASE WHEN mobile_verified_at IS NOT NULL THEN um_user_id END) AS mobile_verified_count
	, COUNT(DISTINCT CASE WHEN kyc_start IS NOT NULL THEN um_user_id END) AS kyc_start_countt
	, COUNT(DISTINCT CASE WHEN level_increase_status = 'pass' AND kyc_passed IS NOT NULL THEN um_user_id END) AS kyc_passed_count
	, COUNT(DISTINCT CASE WHEN has_deposited = TRUE THEN um_user_id END) AS deposited_count
	, COUNT(DISTINCT CASE WHEN has_traded = TRUE THEN um_user_id END) AS traded_count
FROM 
	base 
WHERE signup_hostcountry = 'ID' 
AND register_date >= DATE_TRUNC('day', NOW()) - '7 day'::INTERVAL
GROUP BY 1,2,3,4
ORDER BY 1 DESC 