-- users_funnel --> register --> kyc --> bankbook verified --> deposit 
WITH deposit_temp AS (
SELECT *
	, RANK() OVER(PARTITION BY ap_account_id ORDER BY created_at) rank_ 
FROM analytics.deposit_tickets_master d 
WHERE status = 'FullyProcessed'
), temp_ AS (
SELECT 
	u.signup_hostcountry 
	, u.user_id 
	, u.ap_user_id 
	, u.ap_account_id 
	, u.created_at AS register_date 
	, u.onfido_completed_at as kyc_date 
	, u.email_verified_at AS email_verified_date 
	, u.zipup_subscribed_at AS zipup_date 
	, d.created_at AS first_deposit_date 
	, u.first_traded_at AS firt_traded_date 
	, b.is_verified_at  AS bankbook_verified_date
	, b.review_status 
	, u.is_verified 
	, u.is_email_verified 
	FROM analytics.users_master u 
		LEFT JOIN deposit_temp d 
			ON u.ap_account_id = d.ap_account_id AND d.rank_ = 1 
		LEFT JOIN user_app_public.bank_accounts b 
			ON u.user_id = b.user_id 
--	WHERE signup_hostcountry NOT IN ( 'test','error','xbullion') 
),temp_m AS (
	SELECT 
		signup_hostcountry
		, date_trunc('month', register_date) as register_date
		, COUNT(DISTINCT user_id) AS user_register_count 
	FROM temp_
	GROUP BY 1, 2
),temp_email AS (
	SELECT 
		signup_hostcountry
		, date_trunc('month', email_verified_date) AS email_verified_date
		, COUNT(DISTINCT CASE WHEN email_verified_date IS NOT NULL THEN user_id END) AS email_verified_count 
	FROM temp_
	GROUP BY 1,2
),temp_kyc AS (
	SELECT 
		signup_hostcountry
		, date_trunc('month', kyc_date) AS kyc_date
		, COUNT(DISTINCT CASE WHEN kyc_date IS NOT NULL AND is_verified = TRUE THEN user_id END) AS user_kyc_count 
	FROM temp_
	GROUP BY 1,2
), temp_deposit AS (
	SELECT 
		signup_hostcountry 
		, date_trunc('month', first_deposit_date) first_deposit_date
		, COUNT(DISTINCT CASE WHEN first_deposit_date IS NOT NULL THEN user_id END) AS user_deposit_count 
	FROM temp_ 
	GROUP BY 1,2 
), temp_trade AS (
	SELECT 
		signup_hostcountry 
		, date_trunc('month', firt_traded_date) firt_traded_date
		, COUNT(DISTINCT CASE WHEN firt_traded_date IS NOT NULL THEN user_id END) AS user_traded_count 
	FROM temp_ 
	GROUP BY 1,2 
),temp_zipup as (
	SELECT 
		signup_hostcountry 
		, date_trunc('month', zipup_date) AS zipup_date
		, COUNT(DISTINCT CASE WHEN zipup_date IS NOT NULL THEN user_id END) AS user_zipup_count 
	FROM temp_
	GROUP BY 1,2
),temp_bankbook as (
	SELECT 
		signup_hostcountry 
		, date_trunc('month', bankbook_verified_date) AS bankbook_verified_date
		, COUNT(DISTINCT CASE WHEN bankbook_verified_date IS NOT NULL THEN user_id END) AS user_bankbook_count 
	FROM temp_
	GROUP BY 1,2
), final_temp AS (
SELECT 
	 b.register_date 
	, b.signup_hostcountry
	, COALESCE(user_register_count,0) user_register_count
	, COALESCE(email_verified_count,0) email_verified_count
	, COALESCE(user_kyc_count,0) user_kyc_count 
	, COALESCE(user_deposit_count,0) user_deposit_count
	, COALESCE(user_traded_count,0) user_traded_count
	, COALESCE(user_zipup_count,0) user_zipup_count 
	, COALESCE(user_bankbook_count,0) user_bankbook_count 
FROM temp_m b
	LEFT JOIN temp_email e ON e.signup_hostcountry = b.signup_hostcountry AND e.email_verified_date = b.register_date 
	LEFT JOIN temp_kyc k ON k.signup_hostcountry = b.signup_hostcountry AND k.kyc_date = b.register_date 
	LEFT JOIN temp_deposit d ON d.signup_hostcountry = b.signup_hostcountry AND d.first_deposit_date = b.register_date 
	LEFT JOIN temp_trade t ON t.signup_hostcountry = b.signup_hostcountry AND t.firt_traded_date = b.register_date 
	LEFT JOIN temp_zipup z ON z.signup_hostcountry = b.signup_hostcountry AND z.zipup_date = b.register_date 
	LEFT JOIN temp_bankbook a ON a.signup_hostcountry = b.signup_hostcountry AND a.bankbook_verified_date = b.register_date 
ORDER BY 2,1
)
SELECT *
	, sum(user_register_count) OVER (PARTITION BY signup_hostcountry ORDER BY register_date ) AS total_registered_user
	, sum(email_verified_count) OVER (PARTITION BY signup_hostcountry ORDER BY register_date) AS total_email_verified_user
	, sum(user_kyc_count) OVER (PARTITION BY signup_hostcountry ORDER BY register_date) AS total_kyc_user
	, sum(user_deposit_count) OVER (PARTITION BY signup_hostcountry ORDER BY register_date) AS total_deposit_user
	, sum(user_traded_count) OVER (PARTITION BY signup_hostcountry ORDER BY register_date) AS total_traded_user
	, sum(user_zipup_count) OVER (PARTITION BY signup_hostcountry ORDER BY register_date) AS total_zipup_user
	, sum(user_bankbook_count) OVER (PARTITION BY signup_hostcountry ORDER BY register_date) AS total_bankbook_user
FROM final_temp 
--WHERE signup_hostcountry = 'TH' ---- filter COUNTRY 
ORDER BY 2,1 


SELECT review_status 
	, COUNT(DISTINCT user_id)
FROM user_app_public.bank_accounts b
--WHERE is_verified_at IS NOT NULL 
GROUP BY 1 